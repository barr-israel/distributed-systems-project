use reqwest::blocking::Client as HttpClient;
use serde::Deserialize;

use crate::Document;

const SERVER_ADDRESSES: [&str; 3] = [
    "http://localhost:8080",
    "http://localhost:8081",
    "http://localhost:8082",
];
const LIST_ADDRESSES: [&str; 3] = [
    "http://localhost:8080/list",
    "http://localhost:8081/list",
    "http://localhost:8082/list",
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
pub struct RevisionReadResponse {
    revision: u64,
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
    fn next_server(&mut self) -> usize {
        let next = self.next_server_to_use;
        self.next_server_to_use = (self.next_server_to_use + 1) % SERVER_ADDRESSES.len();
        next
    }
    pub fn new() -> Client {
        Client {
            next_server_to_use: 0,
            http_client: HttpClient::builder().build().unwrap(),
        }
    }
    pub fn read_keys(&mut self) -> Vec<KeyReply> {
        loop {
            let next = self.next_server();
            if let Ok(r) = self
                .http_client
                .get(LIST_ADDRESSES[next])
                .header("Linearized", "1")
                .send()
                .and_then(|r| r.json())
            {
                return r;
            }
        }
    }
    pub fn read_document(&mut self, doc_name: &str) -> DocumentReadResponse {
        let doc_name = doc_name.replace("\\", "\\\\");
        loop {
            let next = self.next_server();
            if let Ok(r) = self
                .http_client
                .get(SERVER_ADDRESSES[next])
                .header("key", &doc_name)
                .header("Linearized", "1")
                .send()
                .and_then(|r| r.json::<DocumentReadResponse>())
            {
                // r.value = r.value.replace("\\\\","\\")
                return r;
            }
        }
    }
    pub fn read_revision(&mut self, doc_name: &str) -> u64 {
        let doc_name = doc_name.replace("\\", "\\\\");
        loop {
            let next = self.next_server();
            if let Ok(r) = self
                .http_client
                .get(SERVER_ADDRESSES[next])
                .header("key", &doc_name)
                .header("Linearized", "1")
                .header("Revision-Only", "1")
                .send()
                .and_then(|r| r.json::<RevisionReadResponse>())
            {
                return r.revision;
            }
        }
    }
    pub fn delete_document(&mut self, doc_name: &str, revision: Option<u64>) -> Result<u64, u64> {
        let doc_name = doc_name.replace("\\", "\\\\");
        loop {
            let next = self.next_server();
            let mut req = self
                .http_client
                .delete(SERVER_ADDRESSES[next])
                .header("key", &doc_name);
            if let Some(rev) = revision {
                req = req.header("revision", rev);
            }
            if let Ok(r) = req.send().and_then(|r| r.json::<WriteResponse>()) {
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
        let doc_name = doc_name.replace("\\", "\\\\");
        let content = content
            .replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r");
        loop {
            let next = self.next_server();
            let mut req = self
                .http_client
                .put(SERVER_ADDRESSES[next])
                .header("key", &doc_name)
                .header("value", &content);
            if let Some(rev) = revision {
                req = req.header("revision", rev);
            }
            if let Ok(r) = req.send().and_then(|r| r.json::<WriteResponse>()) {
                if r.success {
                    return Ok(r.revision);
                } else {
                    return Err(r.revision);
                }
            }
        }
    }
}
