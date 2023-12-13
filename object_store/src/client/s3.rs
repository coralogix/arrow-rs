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

//! The list and multipart API used by both GCS and S3

use crate::multipart::PartId;
use crate::path::Path;
use crate::{ListResult, ObjectMeta, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde::ser::SerializeStruct;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListResponse {
    #[serde(default)]
    pub contents: Vec<ListContents>,
    #[serde(default)]
    pub common_prefixes: Vec<ListPrefix>,
    #[serde(default)]
    pub next_continuation_token: Option<String>,
}

impl TryFrom<ListResponse> for ListResult {
    type Error = crate::Error;

    fn try_from(value: ListResponse) -> Result<Self> {
        let common_prefixes = value
            .common_prefixes
            .into_iter()
            .map(|x| Ok(Path::parse(x.prefix)?))
            .collect::<Result<_>>()?;

        let objects = value
            .contents
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_>>()?;

        Ok(Self {
            common_prefixes,
            objects,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListPrefix {
    pub prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListContents {
    pub key: String,
    pub size: usize,
    pub last_modified: DateTime<Utc>,
    #[serde(rename = "ETag")]
    pub e_tag: Option<String>,
}

impl TryFrom<ListContents> for ObjectMeta {
    type Error = crate::Error;

    fn try_from(value: ListContents) -> Result<Self> {
        Ok(Self {
            location: Path::parse(value.key)?,
            last_modified: value.last_modified,
            size: value.size,
            e_tag: value.e_tag,
            version: None,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InitiateMultipartUploadResult {
    pub upload_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CompleteMultipartUpload {
    pub part: Vec<MultipartPart>,
}

impl From<Vec<PartId>> for CompleteMultipartUpload {
    fn from(value: Vec<PartId>) -> Self {
        let part = value
            .into_iter()
            .enumerate()
            .map(|(part_number, part)| MultipartPart {
                e_tag: part.content_id,
                part_number: part_number + 1,
            })
            .collect();
        Self { part }
    }
}

#[derive(Debug)]
pub struct MultipartPart {
    pub e_tag: String,
    pub part_number: usize,
}

impl Serialize for MultipartPart {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("Part", 2)?;
        s.serialize_field("ETag", format!("\"{}\"", &self.e_tag).as_str())?;
        s.serialize_field("PartNumber", &self.part_number)?;
        s.end()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CompleteMultipartUploadResult {
    #[serde(rename = "ETag")]
    pub e_tag: String,
}

[cfg(test)]
mod tests {
    use crate::aws::client::{CompleteMultipart, MultipartPart};
    use quick_xml;

    #[test]
    fn test_multipart_serialization() {
        let request = CompleteMultipart {
            part: vec![
                MultipartPart {
                    e_tag: "1".to_string(),
                    part_number: 1,
                },
                MultipartPart {
                    e_tag: "2".to_string(),
                    part_number: 2,
                },
                MultipartPart {
                    e_tag: "3".to_string(),
                    part_number: 3,
                },
            ],
        };

        let body = quick_xml::se::to_string(&request).unwrap();

        assert_eq!(
            body,
            r#"<CompleteMultipartUpload><Part><ETag>"1"</ETag><PartNumber>1</PartNumber></Part><Part><ETag>"2"</ETag><PartNumber>2</PartNumber></Part><Part><ETag>"3"</ETag><PartNumber>3</PartNumber></Part></CompleteMultipartUpload>"#
        )
    }
}
