use std::collections::{BTreeMap, HashMap};

use reqwest::blocking::Client as HttpClient;
use serde::Deserialize;

use crate::document::Document;

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
    pub value: String,
    pub revision: u64,
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
    pub fn read_keys(&mut self) -> Vec<Document> {
        let next = self.next_server();
        self.http_client
            .get(LIST_ADDRESSES[next])
            .send()
            .unwrap()
            .json()
            .unwrap()
    }
    pub fn read_document(&mut self, doc_name: &str) -> DocumentReadResponse {
        let next = self.next_server();
        self.http_client
            .get(SERVER_ADDRESSES[next])
            .header("key", doc_name)
            .send()
            .unwrap()
            .json::<DocumentReadResponse>()
            .unwrap()
    }
    pub fn read_revision(&mut self, doc_name: &str) -> u64 {
        todo!()
    }
    pub fn delete_document(&mut self, doc_name: &str, revision: u64) -> u64 {
        todo!()
    }
    pub fn write_document(
        &mut self,
        doc_name: &str,
        content: &str,
        revision: u64,
    ) -> Result<u64, u64> {
        todo!()
    }
}
