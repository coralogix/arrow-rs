// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::RowGroups;
use crate::arrow::arrow_reader::RowSelection;
use crate::column::page::{PageIterator, PageReader};
use crate::errors::ParquetError;
use crate::file::metadata::{ParquetMetaData, RowGroupMetaData};
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::file::reader::{ChunkReader, Length, SerializedPageReader};
use bytes::{Buf, Bytes};
use std::ops::Range;
use std::sync::Arc;

/// An in-memory collection of column chunks
#[derive(Debug)]
pub(crate) struct InMemoryRowGroup<'a> {
    pub(crate) offset_index: Option<&'a [OffsetIndexMetaData]>,
    /// Column chunks for this row group
    pub(crate) column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
    pub(crate) row_count: usize,
    pub(crate) row_group_idx: usize,
    pub(crate) metadata: &'a ParquetMetaData,
}

/// What ranges to fetch for the columns in this row group
#[derive(Debug)]
pub(crate) struct FetchRanges {
    /// The byte ranges to fetch
    pub(crate) ranges: Vec<Range<u64>>,
    /// If `Some`, the start offsets of each page for each column chunk
    pub(crate) page_start_offsets: Option<Vec<Vec<u64>>>,
}

impl InMemoryRowGroup<'_> {
    /// Returns the byte ranges to fetch for the columns specified in
    /// `projection` and `selection`.
    ///
    /// `cache_mask` indicates which columns, if any, are being cached by
    /// [`RowGroupCache`](crate::arrow::array_reader::RowGroupCache).
    /// The `selection` for Cached columns is expanded to batch boundaries to simplify
    /// accounting for what data is cached.
    pub(crate) fn fetch_ranges(
        &self,
        projection: &ProjectionMask,
        selection: Option<&RowSelection>,
        batch_size: usize,
        cache_mask: Option<&ProjectionMask>,
    ) -> FetchRanges {
        let metadata = self.metadata.row_group(self.row_group_idx);
        // A sparse (page-level) fetch can only locate a column's data pages when
        // the offset index actually carries page locations for it. If any column
        // we still need to fetch is missing its page locations we cannot build a
        // valid sparse chunk for it: `scan_ranges` would return no ranges, the
        // column would be fetched empty, and the later read would fail with
        // "Invalid offset in sparse column chunk data: <n>, no matching page
        // found". In that case fall back to fetching whole column chunks (dense).
        let sparse_fetch_possible = |offset_index: &[OffsetIndexMetaData]| {
            self.column_chunks.iter().enumerate().all(|(idx, chunk)| {
                chunk.is_some()
                    || !projection.leaf_included(idx)
                    || !offset_index[idx].page_locations.is_empty()
            })
        };
        if let Some((selection, offset_index)) = selection
            .zip(self.offset_index)
            .filter(|(_, offset_index)| sparse_fetch_possible(offset_index))
        {
            let expanded_selection =
                selection.expand_to_batch_boundaries(batch_size, self.row_count);

            // If we have a `RowSelection` and an `OffsetIndex` then only fetch
            // pages required for the `RowSelection`
            // Consider preallocating outer vec: https://github.com/apache/arrow-rs/issues/8667
            let mut page_start_offsets: Vec<Vec<u64>> = vec![];

            let ranges = self
                .column_chunks
                .iter()
                .zip(metadata.columns())
                .enumerate()
                .filter(|&(idx, (chunk, _chunk_meta))| {
                    chunk.is_none() && projection.leaf_included(idx)
                })
                .flat_map(|(idx, (_chunk, chunk_meta))| {
                    // If the first page does not start at the beginning of the column,
                    // then we need to also fetch a dictionary page.
                    let mut ranges: Vec<Range<u64>> = vec![];
                    let (start, _len) = chunk_meta.byte_range();
                    match offset_index[idx].page_locations.first() {
                        Some(first) if first.offset as u64 != start => {
                            ranges.push(start..first.offset as u64);
                        }
                        _ => (),
                    }

                    // Expand selection to batch boundaries if needed for caching
                    // (see doc comment for this function for details on `cache_mask`)
                    let use_expanded = cache_mask.map(|m| m.leaf_included(idx)).unwrap_or(false);
                    if use_expanded {
                        ranges.extend(
                            expanded_selection.scan_ranges(&offset_index[idx].page_locations),
                        );
                    } else {
                        ranges.extend(selection.scan_ranges(&offset_index[idx].page_locations));
                    }
                    page_start_offsets.push(ranges.iter().map(|range| range.start).collect());

                    ranges
                })
                .collect();
            FetchRanges {
                ranges,
                page_start_offsets: Some(page_start_offsets),
            }
        } else {
            let ranges = self
                .column_chunks
                .iter()
                .enumerate()
                .filter(|&(idx, chunk)| chunk.is_none() && projection.leaf_included(idx))
                .map(|(idx, _chunk)| {
                    let column = metadata.column(idx);
                    let (start, length) = column.byte_range();
                    start..(start + length)
                })
                .collect();
            FetchRanges {
                ranges,
                page_start_offsets: None,
            }
        }
    }

    /// Fills in `self.column_chunks` with the data fetched from `chunk_data`.
    ///
    /// This function **must** be called with the data from the ranges returned by
    /// `fetch_ranges` and the corresponding page_start_offsets, with the exact same and `selection`.
    pub(crate) fn fill_column_chunks<I>(
        &mut self,
        projection: &ProjectionMask,
        page_start_offsets: Option<Vec<Vec<u64>>>,
        chunk_data: I,
    ) where
        I: IntoIterator<Item = Bytes>,
    {
        let mut chunk_data = chunk_data.into_iter();
        let metadata = self.metadata.row_group(self.row_group_idx);
        if let Some(page_start_offsets) = page_start_offsets {
            // If we have a `RowSelection` and an `OffsetIndex` then only fetch pages required for the
            // `RowSelection`
            let mut page_start_offsets = page_start_offsets.into_iter();

            for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
                if chunk.is_some() || !projection.leaf_included(idx) {
                    continue;
                }

                if let Some(offsets) = page_start_offsets.next() {
                    let mut chunks = Vec::with_capacity(offsets.len());
                    for _ in 0..offsets.len() {
                        chunks.push(chunk_data.next().unwrap());
                    }

                    *chunk = Some(Arc::new(ColumnChunkData::Sparse {
                        length: metadata.column(idx).byte_range().1 as usize,
                        data: offsets
                            .into_iter()
                            .map(|x| x as usize)
                            .zip(chunks.into_iter())
                            .collect(),
                    }))
                }
            }
        } else {
            for (idx, chunk) in self.column_chunks.iter_mut().enumerate() {
                if chunk.is_some() || !projection.leaf_included(idx) {
                    continue;
                }

                if let Some(data) = chunk_data.next() {
                    *chunk = Some(Arc::new(ColumnChunkData::Dense {
                        offset: metadata.column(idx).byte_range().0 as usize,
                        data,
                    }));
                }
            }
        }
    }
}

impl RowGroups for InMemoryRowGroup<'_> {
    fn num_rows(&self) -> usize {
        self.row_count
    }

    /// Return chunks for column i
    fn column_chunks(&self, i: usize) -> crate::errors::Result<Box<dyn PageIterator>> {
        match &self.column_chunks[i] {
            None => Err(ParquetError::General(format!(
                "Invalid column index {i}, column was not fetched"
            ))),
            Some(data) => {
                let page_locations = self
                    .offset_index
                    // filter out empty offset indexes (old versions specified Some(vec![]) when no present)
                    .filter(|index| !index.is_empty())
                    .map(|index| index[i].page_locations.clone());
                let column_chunk_metadata = self.metadata.row_group(self.row_group_idx).column(i);
                let page_reader = SerializedPageReader::new(
                    data.clone(),
                    column_chunk_metadata,
                    self.row_count,
                    page_locations,
                )?;
                let page_reader = page_reader.add_crypto_context(
                    self.row_group_idx,
                    i,
                    self.metadata,
                    column_chunk_metadata,
                )?;

                let page_reader: Box<dyn PageReader> = Box::new(page_reader);

                Ok(Box::new(ColumnChunkIterator {
                    reader: Some(Ok(page_reader)),
                }))
            }
        }
    }

    fn row_groups(&self) -> Box<dyn Iterator<Item = &RowGroupMetaData> + '_> {
        Box::new(std::iter::once(self.metadata.row_group(self.row_group_idx)))
    }

    fn metadata(&self) -> &ParquetMetaData {
        self.metadata
    }
}

/// An in-memory column chunk.
/// This allows us to hold either dense column chunks or sparse column chunks and easily
/// access them by offset.
#[derive(Clone, Debug)]
pub(crate) enum ColumnChunkData {
    /// Column chunk data representing only a subset of data pages.
    /// For example if a row selection (possibly caused by a filter in a query) causes us to read only
    /// a subset of the rows in the column.
    Sparse {
        /// Length of the full column chunk
        length: usize,
        /// Subset of data pages included in this sparse chunk.
        ///
        /// Each element is a tuple of (page offset within file, page data).
        /// Each entry is a complete page and the list is ordered by offset.
        data: Vec<(usize, Bytes)>,
    },
    /// Full column chunk and the offset within the original file
    Dense { offset: usize, data: Bytes },
}

impl ColumnChunkData {
    /// Return the data for this column chunk at the given offset
    fn get(&self, start: u64) -> crate::errors::Result<Bytes> {
        match &self {
            ColumnChunkData::Sparse { data, .. } => data
                .binary_search_by_key(&start, |(offset, _)| *offset as u64)
                .map(|idx| data[idx].1.clone())
                .map_err(|_| {
                    ParquetError::General(format!(
                        "Invalid offset in sparse column chunk data: {start}, no matching page found.\
                         If you are using a `SelectionStrategyPolicy::Mask`, ensure that the OffsetIndex is provided when \
                         creating the InMemoryRowGroup."
                    ))
                }),
            ColumnChunkData::Dense { offset, data } => {
                let start = start as usize - *offset;
                Ok(data.slice(start..))
            }
        }
    }
}

impl Length for ColumnChunkData {
    /// Return the total length of the full column chunk
    fn len(&self) -> u64 {
        match &self {
            ColumnChunkData::Sparse { length, .. } => *length as u64,
            ColumnChunkData::Dense { data, .. } => data.len() as u64,
        }
    }
}

impl ChunkReader for ColumnChunkData {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> crate::errors::Result<Self::T> {
        Ok(self.get(start)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> crate::errors::Result<Bytes> {
        Ok(self.get(start)?.slice(..length))
    }
}

/// Implements [`PageIterator`] for a single column chunk, yielding a single [`PageReader`]
struct ColumnChunkIterator {
    reader: Option<crate::errors::Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = crate::errors::Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::{RowSelection, RowSelector};
    use crate::basic::Type as PhysicalType;
    use crate::file::metadata::{
        ColumnChunkMetaData, FileMetaData, ParquetMetaDataBuilder, RowGroupMetaData,
    };
    use crate::file::page_index::offset_index::PageLocation;
    use crate::schema::types::{SchemaDescriptor, Type};

    /// Metadata for a single row group with two INT32 column chunks occupying
    /// `[4, 104)` and `[200, 300)` in the file.
    fn two_column_metadata() -> (ParquetMetaData, Arc<SchemaDescriptor>) {
        let a = Arc::new(
            Type::primitive_type_builder("a", PhysicalType::INT32)
                .build()
                .unwrap(),
        );
        let b = Arc::new(
            Type::primitive_type_builder("b", PhysicalType::INT32)
                .build()
                .unwrap(),
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![a, b])
                .build()
                .unwrap(),
        );
        let schema_descr = Arc::new(SchemaDescriptor::new(schema));

        let col0 = ColumnChunkMetaData::builder(schema_descr.column(0))
            .set_total_compressed_size(100)
            .set_data_page_offset(4)
            .build()
            .unwrap();
        let col1 = ColumnChunkMetaData::builder(schema_descr.column(1))
            .set_total_compressed_size(100)
            .set_data_page_offset(200)
            .build()
            .unwrap();

        let row_group = RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(10)
            .set_total_byte_size(300)
            .set_column_metadata(vec![col0, col1])
            .build()
            .unwrap();

        let file_meta = FileMetaData::new(1, 10, None, None, schema_descr.clone(), None);
        let metadata = ParquetMetaDataBuilder::new(file_meta)
            .set_row_groups(vec![row_group])
            .build();
        (metadata, schema_descr)
    }

    fn page_locations() -> OffsetIndexMetaData {
        OffsetIndexMetaData {
            page_locations: vec![
                PageLocation {
                    offset: 4,
                    compressed_page_size: 50,
                    first_row_index: 0,
                },
                PageLocation {
                    offset: 54,
                    compressed_page_size: 50,
                    first_row_index: 5,
                },
            ],
            unencoded_byte_array_data_bytes: None,
        }
    }

    fn empty_locations() -> OffsetIndexMetaData {
        OffsetIndexMetaData {
            page_locations: vec![],
            unencoded_byte_array_data_bytes: None,
        }
    }

    fn selection() -> RowSelection {
        RowSelection::from(vec![RowSelector::skip(3), RowSelector::select(4)])
    }

    #[test]
    fn sparse_fetch_when_every_projected_column_has_page_locations() {
        let (metadata, _schema) = two_column_metadata();
        let offset_index = vec![page_locations(), page_locations()];
        let row_group = InMemoryRowGroup {
            offset_index: Some(&offset_index),
            column_chunks: vec![None, None],
            row_count: 10,
            row_group_idx: 0,
            metadata: &metadata,
        };
        let fetch = row_group.fetch_ranges(&ProjectionMask::all(), Some(&selection()), 8, None);
        assert!(
            fetch.page_start_offsets.is_some(),
            "a complete offset index should drive a sparse (page-level) fetch"
        );
    }

    #[test]
    fn dense_fallback_when_a_projected_column_lacks_page_locations() {
        let (metadata, _schema) = two_column_metadata();
        // Column 1 has no page locations, e.g. metadata synthesized from a
        // manifest that is missing its page index. Previously this made the
        // sparse fetch read an empty column chunk and later fail with "Invalid
        // offset in sparse column chunk data: <n>, no matching page found".
        let offset_index = vec![page_locations(), empty_locations()];
        let row_group = InMemoryRowGroup {
            offset_index: Some(&offset_index),
            column_chunks: vec![None, None],
            row_count: 10,
            row_group_idx: 0,
            metadata: &metadata,
        };
        let fetch = row_group.fetch_ranges(&ProjectionMask::all(), Some(&selection()), 8, None);
        assert!(
            fetch.page_start_offsets.is_none(),
            "a projected column without page locations must fall back to a dense fetch"
        );
        // A dense fetch reads each projected column's full byte range.
        assert_eq!(fetch.ranges, vec![4..104, 200..300]);
    }

    #[test]
    fn sparse_fetch_when_column_without_page_locations_is_not_projected() {
        let (metadata, schema) = two_column_metadata();
        let offset_index = vec![page_locations(), empty_locations()];
        let row_group = InMemoryRowGroup {
            offset_index: Some(&offset_index),
            column_chunks: vec![None, None],
            row_count: 10,
            row_group_idx: 0,
            metadata: &metadata,
        };
        // Only column 0, which has page locations, is projected.
        let projection = ProjectionMask::leaves(schema.as_ref(), [0]);
        let fetch = row_group.fetch_ranges(&projection, Some(&selection()), 8, None);
        assert!(
            fetch.page_start_offsets.is_some(),
            "a missing index on an unprojected column should not disable sparse fetches"
        );
    }
}
