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

use crate::aws::checksum::Checksum;
use crate::aws::credential::{AwsCredential, CredentialExt};
use crate::aws::{
    AwsCredentialProvider, S3CopyIfNotExists, STORE, STRICT_ENCODE_SET,
    STRICT_PATH_ENCODE_SET,
};
use crate::client::get::GetClient;
use crate::client::list::ListClient;
use crate::client::list_response::ListResponse;
use crate::client::retry::RetryExt;
use crate::client::GetOptionsExt;
use crate::multipart::PartId;
use crate::path::DELIMITER;
use crate::{
    ClientOptions, GetOptions, ListResult, MultipartId, Path, Result, RetryConfig,
};
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::{Buf, Bytes};
use itertools::Itertools;
use percent_encoding::{percent_encode, utf8_percent_encode, PercentEncode};
use quick_xml::events::{self as xml_events};
use reqwest::{
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    Client as ReqwestClient, Method, Response, StatusCode,
};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::sync::Arc;

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub(crate) enum Error {
    #[snafu(display("Error performing get request {}: {}", path, source))]
    GetRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error fetching get response body {}: {}", path, source))]
    GetResponseBody {
        source: reqwest::Error,
        path: String,
    },

    #[snafu(display("Error performing put request {}: {}", path, source))]
    PutRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing delete request {}: {}", path, source))]
    DeleteRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing DeleteObjects request: {}", source))]
    DeleteObjectsRequest { source: crate::client::retry::Error },

    #[snafu(display(
        "DeleteObjects request failed for key {}: {} (code: {})",
        path,
        message,
        code
    ))]
    DeleteFailed {
        path: String,
        code: String,
        message: String,
    },

    #[snafu(display("Error getting DeleteObjects response body: {}", source))]
    DeleteObjectsResponse { source: reqwest::Error },

    #[snafu(display("Got invalid DeleteObjects response: {}", source))]
    InvalidDeleteObjectsResponse {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Error performing copy request {}: {}", path, source))]
    CopyRequest {
        source: crate::client::retry::Error,
        path: String,
    },

    #[snafu(display("Error performing list request: {}", source))]
    ListRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting list response body: {}", source))]
    ListResponseBody { source: reqwest::Error },

    #[snafu(display("Error performing create multipart request: {}", source))]
    CreateMultipartRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting create multipart response body: {}", source))]
    CreateMultipartResponseBody { source: reqwest::Error },

    #[snafu(display("Error performing complete multipart request: {}", source))]
    CompleteMultipartRequest { source: crate::client::retry::Error },

    #[snafu(display("Got invalid list response: {}", source))]
    InvalidListResponse { source: quick_xml::de::DeError },

    #[snafu(display("Got invalid multipart response: {}", source))]
    InvalidMultipartResponse { source: quick_xml::de::DeError },
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::GetRequest { source, path }
            | Error::DeleteRequest { source, path }
            | Error::CopyRequest { source, path }
            | Error::PutRequest { source, path } => source.error(STORE, path),
            _ => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipart {
    upload_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase", rename = "CompleteMultipartUpload")]
struct CompleteMultipart {
    part: Vec<MultipartPart>,
}

#[derive(Debug)]
struct MultipartPart {
    e_tag: String,
    part_number: usize,
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

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "DeleteResult")]
struct BatchDeleteResponse {
    #[serde(rename = "$value")]
    content: Vec<DeleteObjectResult>,
}

#[derive(Deserialize)]
enum DeleteObjectResult {
    Deleted(DeletedObject),
    Error(DeleteError),
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "Deleted")]
struct DeletedObject {
    #[allow(dead_code)]
    key: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "Error")]
struct DeleteError {
    key: String,
    code: String,
    message: String,
}

impl From<DeleteError> for Error {
    fn from(err: DeleteError) -> Self {
        Self::DeleteFailed {
            path: err.key,
            code: err.code,
            message: err.message,
        }
    }
}

#[derive(Debug)]
pub struct S3Config {
    pub region: String,
    pub endpoint: String,
    pub bucket: String,
    pub bucket_endpoint: String,
    pub credentials: AwsCredentialProvider,
    pub retry_config: RetryConfig,
    pub client_options: ClientOptions,
    pub sign_payload: bool,
    pub checksum: Option<Checksum>,
    pub copy_if_not_exists: Option<S3CopyIfNotExists>,
}

impl S3Config {
    fn path_url(&self, path: &Path) -> String {
        format!("{}/{}", self.bucket_endpoint, encode_path(path))
    }
}

#[derive(Debug)]
pub(crate) struct S3Client {
    config: S3Config,
    client: ReqwestClient,
}

const TAGGING_HEADER: &str = "x-amz-tagging";

impl S3Client {
    pub fn new(config: S3Config) -> Result<Self> {
        let client = config.client_options.client()?;
        Ok(Self { config, client })
    }

    /// Returns the config
    pub fn config(&self) -> &S3Config {
        &self.config
    }

    async fn get_credential(&self) -> Result<Arc<AwsCredential>> {
        self.config.credentials.get_credential().await
    }

    /// Make an S3 PUT request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
    pub async fn put_request<T: Serialize + ?Sized + Sync>(
        &self,
        path: &Path,
        bytes: Bytes,
        query: &T,
        tags: Option<&HashMap<String, String>>,
    ) -> Result<Response> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);
        let mut builder = self.client.request(Method::PUT, url);
        let mut payload_sha256 = None;

        if let Some(checksum) = self.config().checksum {
            let digest = checksum.digest(&bytes);
            builder =
                builder.header(checksum.header_name(), BASE64_STANDARD.encode(&digest));
            if checksum == Checksum::SHA256 {
                payload_sha256 = Some(digest);
            }
        }

        builder = match bytes.is_empty() {
            true => builder.header(CONTENT_LENGTH, 0), // Handle empty uploads (#4514)
            false => builder.body(bytes),
        };

        if let Some(value) = self.config().client_options.get_content_type(path) {
            builder = builder.header(CONTENT_TYPE, value);
        }

        if let Some(tags) = tags {
            let tags = tags
                .iter()
                .map(|(key, value)| {
                    let key =
                        percent_encode(key.as_bytes(), &STRICT_ENCODE_SET).to_string();
                    let value =
                        percent_encode(value.as_bytes(), &STRICT_ENCODE_SET).to_string();
                    format!("{key}={value}")
                })
                .join("&");
            builder = builder.header(TAGGING_HEADER, tags);
        }

        let response = builder
            .query(query)
            .with_aws_sigv4(
                credential.as_ref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                payload_sha256.as_deref(),
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(PutRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(response)
    }

    /// Make an S3 Delete request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html>
    pub async fn delete_request<T: Serialize + ?Sized + Sync>(
        &self,
        path: &Path,
        query: &T,
    ) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);

        self.client
            .request(Method::DELETE, url)
            .query(query)
            .with_aws_sigv4(
                credential.as_ref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(DeleteRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(())
    }

    /// Make an S3 Delete Objects request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html>
    ///
    /// Produces a vector of results, one for each path in the input vector. If
    /// the delete was successful, the path is returned in the `Ok` variant. If
    /// there was an error for a certain path, the error will be returned in the
    /// vector. If there was an issue with making the overall request, an error
    /// will be returned at the top level.
    pub async fn bulk_delete_request(
        &self,
        paths: Vec<Path>,
    ) -> Result<Vec<Result<Path>>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }

        let credential = self.get_credential().await?;
        let url = format!("{}?delete", self.config.bucket_endpoint);

        let mut buffer = Vec::new();
        let mut writer = quick_xml::Writer::new(&mut buffer);
        writer
            .write_event(xml_events::Event::Start(
                xml_events::BytesStart::new("Delete").with_attributes([(
                    "xmlns",
                    "http://s3.amazonaws.com/doc/2006-03-01/",
                )]),
            ))
            .unwrap();
        for path in &paths {
            // <Object><Key>{path}</Key></Object>
            writer
                .write_event(xml_events::Event::Start(xml_events::BytesStart::new(
                    "Object",
                )))
                .unwrap();
            writer
                .write_event(xml_events::Event::Start(xml_events::BytesStart::new("Key")))
                .unwrap();
            writer
                .write_event(xml_events::Event::Text(xml_events::BytesText::new(
                    path.as_ref(),
                )))
                .map_err(|err| crate::Error::Generic {
                    store: STORE,
                    source: Box::new(err),
                })?;
            writer
                .write_event(xml_events::Event::End(xml_events::BytesEnd::new("Key")))
                .unwrap();
            writer
                .write_event(xml_events::Event::End(xml_events::BytesEnd::new("Object")))
                .unwrap();
        }
        writer
            .write_event(xml_events::Event::End(xml_events::BytesEnd::new("Delete")))
            .unwrap();

        let body = Bytes::from(buffer);

        let mut builder = self.client.request(Method::POST, url);

        // Compute checksum - S3 *requires* this for DeleteObjects requests, so we default to
        // their algorithm if the user hasn't specified one.
        let checksum = self.config().checksum.unwrap_or(Checksum::SHA256);
        let digest = checksum.digest(&body);
        builder = builder.header(checksum.header_name(), BASE64_STANDARD.encode(&digest));
        let payload_sha256 = if checksum == Checksum::SHA256 {
            Some(digest)
        } else {
            None
        };

        let response = builder
            .header(CONTENT_TYPE, "application/xml")
            .body(body)
            .with_aws_sigv4(
                credential.as_ref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                payload_sha256.as_deref(),
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(DeleteObjectsRequestSnafu {})?
            .bytes()
            .await
            .context(DeleteObjectsResponseSnafu {})?;

        let response: BatchDeleteResponse = quick_xml::de::from_reader(response.reader())
            .map_err(|err| Error::InvalidDeleteObjectsResponse {
                source: Box::new(err),
            })?;

        // Assume all were ok, then fill in errors. This guarantees output order
        // matches input order.
        let mut results: Vec<Result<Path>> = paths.iter().cloned().map(Ok).collect();
        for content in response.content.into_iter() {
            if let DeleteObjectResult::Error(error) = content {
                let path = Path::parse(&error.key).map_err(|err| {
                    Error::InvalidDeleteObjectsResponse {
                        source: Box::new(err),
                    }
                })?;
                let i = paths.iter().find_position(|&p| p == &path).unwrap().0;
                results[i] = Err(Error::from(error).into());
            }
        }

        Ok(results)
    }

    /// Make an S3 Copy request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html>
    pub async fn copy_request(
        &self,
        from: &Path,
        to: &Path,
        overwrite: bool,
    ) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(to);
        let source = format!("{}/{}", self.config.bucket, encode_path(from));

        let mut builder = self
            .client
            .request(Method::PUT, url)
            .header("x-amz-copy-source", source);

        if !overwrite {
            match &self.config.copy_if_not_exists {
                Some(S3CopyIfNotExists::Header(k, v)) => {
                    builder = builder.header(k, v);
                }
                None => {
                    return Err(crate::Error::NotSupported {
                        source: "S3 does not support copy-if-not-exists"
                            .to_string()
                            .into(),
                    })
                }
            }
        }

        builder
            .with_aws_sigv4(
                credential.as_ref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .map_err(|source| match source.status() {
                Some(StatusCode::PRECONDITION_FAILED) => crate::Error::AlreadyExists {
                    source: Box::new(source),
                    path: to.to_string(),
                },
                _ => Error::CopyRequest {
                    source,
                    path: from.to_string(),
                }
                .into(),
            })?;

        Ok(())
    }

    pub async fn create_multipart(&self, location: &Path) -> Result<MultipartId> {
        let credential = self.get_credential().await?;
        let url = format!("{}?uploads=", self.config.path_url(location),);

        let response = self
            .client
            .request(Method::POST, url)
            .with_aws_sigv4(
                credential.as_ref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(CreateMultipartRequestSnafu)?
            .bytes()
            .await
            .context(CreateMultipartResponseBodySnafu)?;

        let response: InitiateMultipart = quick_xml::de::from_reader(response.reader())
            .context(InvalidMultipartResponseSnafu)?;

        Ok(response.upload_id)
    }

    pub async fn complete_multipart(
        &self,
        location: &Path,
        upload_id: &str,
        parts: Vec<PartId>,
    ) -> Result<()> {
        let parts = parts
            .into_iter()
            .enumerate()
            .map(|(part_idx, part)| MultipartPart {
                e_tag: part.content_id,
                part_number: part_idx + 1,
            })
            .collect();

        let request = CompleteMultipart { part: parts };
        let body = quick_xml::se::to_string(&request).unwrap();

        let credential = self.get_credential().await?;
        let url = self.config.path_url(location);

        self.client
            .request(Method::POST, url)
            .query(&[("uploadId", upload_id)])
            .body(body)
            .with_aws_sigv4(
                credential.as_ref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(CompleteMultipartRequestSnafu)?;

        Ok(())
    }
}

#[async_trait]
impl GetClient for S3Client {
    const STORE: &'static str = STORE;

    /// Make an S3 GET request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html>
    async fn get_request(
        &self,
        path: &Path,
        options: GetOptions,
        head: bool,
    ) -> Result<Response> {
        let credential = self.get_credential().await?;
        let url = self.config.path_url(path);
        let method = match head {
            true => Method::HEAD,
            false => Method::GET,
        };

        let builder = self.client.request(method, url);

        let response = builder
            .with_get_options(options)
            .with_aws_sigv4(
                credential.as_ref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(GetRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(response)
    }
}

#[async_trait]
impl ListClient for S3Client {
    /// Make an S3 List request <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html>
    async fn list_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        token: Option<&str>,
        offset: Option<&str>,
    ) -> Result<(ListResult, Option<String>)> {
        let credential = self.get_credential().await?;
        let url = self.config.bucket_endpoint.clone();

        let mut query = Vec::with_capacity(4);

        if let Some(token) = token {
            query.push(("continuation-token", token))
        }

        if delimiter {
            query.push(("delimiter", DELIMITER))
        }

        query.push(("list-type", "2"));

        if let Some(prefix) = prefix {
            query.push(("prefix", prefix))
        }

        if let Some(offset) = offset {
            query.push(("start-after", offset))
        }

        let response = self
            .client
            .request(Method::GET, &url)
            .query(&query)
            .with_aws_sigv4(
                credential.as_ref(),
                &self.config.region,
                "s3",
                self.config.sign_payload,
                None,
            )
            .send_retry(&self.config.retry_config)
            .await
            .context(ListRequestSnafu)?
            .bytes()
            .await
            .context(ListResponseBodySnafu)?;

        let mut response: ListResponse = quick_xml::de::from_reader(response.reader())
            .context(InvalidListResponseSnafu)?;
        let token = response.next_continuation_token.take();

        Ok((response.try_into()?, token))
    }
}

fn encode_path(path: &Path) -> PercentEncode<'_> {
    utf8_percent_encode(path.as_ref(), &STRICT_PATH_ENCODE_SET)
}

#[cfg(test)]
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
