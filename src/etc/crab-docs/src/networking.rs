use reqwest::blocking::Client as HttpClient;
use serde::Deserialize;

use crate::Document;

const SERVER_ADDRESSES: [&str; 5] = [
    "http://localhost:8080",
    "http://localhost:8081",
    "http://localhost:8082",
    "http://localhost:8083",
    "http://localhost:8084",
];

#[derive(Debug, Deserialize)]
pub struct DocumentReadResponse {
    pub value: Option<String>,
    pub revision: u64,
}

#[derive(Debug, Deserialize)]
pub struct KeyReply {
    pub key: String,
    pub revision: u64,
}

impl KeyReply {
    pub fn into_doc(self, unseen_changes: bool) -> Document {
        Document {
            name: self.key,
            revision: self.revision,
            unseen_changes,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct WriteResponse {
    success: bool,
    revision: u64,
}

#[derive(Debug)]
pub struct Client {
    next_server_to_use: usize,
    http_client: HttpClient,
}

impl Client {
    fn next_server(&mut self) -> &'static str {
        let next = self.next_server_to_use;
        self.next_server_to_use = (self.next_server_to_use + 1) % SERVER_ADDRESSES.len();
        SERVER_ADDRESSES[next]
    }
    pub fn new() -> Client {
        Client {
            next_server_to_use: 0,
            http_client: HttpClient::builder().build().unwrap(),
        }
    }
    pub fn read_keys(&mut self) -> Vec<KeyReply> {
        let params = &[("linearized", "1")];
        loop {
            let next = self.next_server();
            let url =
                reqwest::Url::parse_with_params(format!("{}/keys", next).as_str(), params).unwrap();
            if let Ok(r) = self.http_client.get(url).send().and_then(|r| r.json()) {
                return r;
            }
        }
    }
    pub fn read_document(&mut self, doc_name: &str) -> DocumentReadResponse {
        // let doc_name = doc_name.replace("\\", "\\\\");
        loop {
            let next = self.next_server();
            let url = reqwest::Url::parse_with_params(
                format!("{}/keys/{doc_name}", next).as_str(),
                &[("linearized", "1")],
            )
            .unwrap();
            if let Ok(r) = self
                .http_client
                .get(url)
                .header("Linearized", "1")
                .send()
                .and_then(|r| r.json::<DocumentReadResponse>())
            {
                return r;
            }
        }
    }
    pub fn delete_document(&mut self, doc_name: &str, revision: Option<u64>) -> Result<u64, u64> {
        // let doc_name = doc_name.replace("\\", "\\\\");
        let params: &[(&str, String)] = if let Some(rev) = revision {
            &[("revision", rev.to_string())]
        } else {
            &[]
        };
        loop {
            let next = self.next_server();
            let url = reqwest::Url::parse_with_params(
                format!("{}/keys/{doc_name}", next).as_str(),
                params,
            )
            .unwrap();
            if let Ok(r) = self
                .http_client
                .delete(url)
                .send()
                .and_then(|r| r.json::<WriteResponse>())
            {
                if r.success {
                    return Ok(r.revision);
                } else {
                    return Err(r.revision);
                }
            }
        }
    }
    pub fn write_document(
        &mut self,
        doc_name: &str,
        content: &str,
        revision: Option<u64>,
    ) -> Result<u64, u64> {
        // let doc_name = doc_name.replace("\\", "\\\\");
        let content = content
            .replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r");
        let params: &[(&str, String)] = if let Some(rev) = revision {
            &[("revision", rev.to_string())]
        } else {
            &[]
        };
        loop {
            let next = self.next_server();
            let url = reqwest::Url::parse_with_params(
                format!("{}/keys/{doc_name}", next).as_str(),
                params,
            )
            .unwrap();
            if let Ok(r) = self
                .http_client
                .put(url)
                .body(content.to_string())
                .send()
                .and_then(|r| r.json::<WriteResponse>())
            {
                if r.success {
                    return Ok(r.revision);
                } else {
                    return Err(r.revision);
                }
            }
        }
    }
}
