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

use crate::multipart::PartId;
use parking_lot::Mutex;

/// An interior mutable collection of upload parts and their corresponding part index
#[derive(Debug, Default)]
pub(crate) struct Parts(Mutex<Vec<(usize, PartId)>>);

impl Parts {
    /// Record the [`PartId`] for a given index
    ///
    /// Note: calling this method multiple times with the same `part_idx`
    /// will result in multiple [`PartId`] in the final output
    pub(crate) fn put(&self, part_idx: usize, id: PartId) {
        self.0.lock().push((part_idx, id))
    }

    /// Produce the final list of [`PartId`] ordered by `part_idx`
    ///
    /// `expected` is the number of parts expected in the final result
    pub(crate) fn finish(&self, expected: usize) -> crate::Result<Vec<PartId>> {
        let mut parts = self.0.lock();
        if parts.len() != expected {
            return Err(crate::Error::Generic {
                store: "Parts",
                source: "Missing part".to_string().into(),
            });
        }
        sort(&mut parts);
        Ok(parts.drain(..).map(|(_, v)| v).collect())
    }
}

fn sort(parts: &mut [(usize, PartId)]) {
    parts.sort_unstable_by(|a, b| match (a, b) {
        ((idx_a, part_a), (idx_b, part_b)) if part_a.size == part_b.size => idx_a.cmp(idx_b),
        ((_, part_a), (_, part_b)) => part_b.size.cmp(&part_a.size),
    });
}

#[cfg(test)]
mod tests {
    use crate::multipart::PartId;

    #[test]
    fn test_sort() {
        let mut parts = vec![
            (
                1,
                PartId {
                    content_id: "1".to_string(),
                    size: 100,
                },
            ),
            (
                2,
                PartId {
                    content_id: "2".to_string(),
                    size: 50,
                },
            ),
            (
                3,
                PartId {
                    content_id: "3".to_string(),
                    size: 100,
                },
            ),
            (
                4,
                PartId {
                    content_id: "4".to_string(),
                    size: 100,
                },
            ),
        ];
        super::sort(&mut parts);

        assert_eq!(parts[0].1.content_id, "1");
        assert_eq!(parts[1].1.content_id, "3");
        assert_eq!(parts[2].1.content_id, "4");
        assert_eq!(parts[3].1.content_id, "2");
    }
}
