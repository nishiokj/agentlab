use anyhow::Result;
use chrono::Utc;
use lab_core::ArtifactStore;
use serde::{Deserialize, Serialize};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use tiny_http::{Response, Server};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceIngestRecord {
    pub timestamp: String,
    pub content_type: Option<String>,
    pub artifact_ref: String,
    pub size_bytes: usize,
}

#[derive(Debug)]
pub struct OtlpReceiver {
    pub endpoint: String,
    server_thread: Option<std::thread::JoinHandle<()>>,
    records: Arc<Mutex<Vec<TraceIngestRecord>>>,
}

impl OtlpReceiver {
    pub fn start(port: u16, artifact_store: ArtifactStore) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", port))?;
        let addr = listener.local_addr()?;
        let server =
            Server::from_listener(listener, None).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let endpoint = format!("http://{}:{}", addr.ip(), addr.port());

        let records = Arc::new(Mutex::new(Vec::new()));
        let records_clone = records.clone();

        let handle = thread::spawn(move || {
            for mut request in server.incoming_requests() {
                let mut content = Vec::new();
                let _ = request.as_reader().read_to_end(&mut content);
                let content_type = request
                    .headers()
                    .iter()
                    .find(|h| h.field.equiv("Content-Type"))
                    .map(|h| h.value.to_string());
                let size = content.len();
                let artifact_ref = artifact_store
                    .put_bytes(&content)
                    .unwrap_or_else(|_| "artifact://sha256/unknown".to_string());
                let record = TraceIngestRecord {
                    timestamp: Utc::now().to_rfc3339(),
                    content_type,
                    artifact_ref,
                    size_bytes: size,
                };
                if let Ok(mut guard) = records_clone.lock() {
                    guard.push(record);
                }
                let _ = request.respond(Response::from_string("ok"));
            }
        });

        Ok(Self {
            endpoint,
            server_thread: Some(handle),
            records,
        })
    }

    pub fn records(&self) -> Vec<TraceIngestRecord> {
        self.records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn stop(mut self) {
        if let Some(handle) = self.server_thread.take() {
            let _ = handle.join();
        }
    }
}
