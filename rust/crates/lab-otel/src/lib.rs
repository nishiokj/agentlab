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

pub struct OtlpReceiver {
    pub endpoint: String,
    server: Arc<Server>,
    server_thread: Option<std::thread::JoinHandle<()>>,
    records: Arc<Mutex<Vec<TraceIngestRecord>>>,
}

impl OtlpReceiver {
    pub fn start(port: u16, artifact_store: ArtifactStore) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", port))?;
        let addr = listener.local_addr()?;
        let server = Arc::new(
            Server::from_listener(listener, None).map_err(|e| anyhow::anyhow!(e.to_string()))?,
        );
        let endpoint = format!("http://{}:{}", addr.ip(), addr.port());

        let records = Arc::new(Mutex::new(Vec::new()));
        let records_clone = records.clone();
        let server_clone = server.clone();

        let handle = thread::spawn(move || {
            for mut request in server_clone.incoming_requests() {
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
            server,
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
        self.server.unblock();
        if let Some(handle) = self.server_thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for OtlpReceiver {
    fn drop(&mut self) {
        self.server.unblock();
        if let Some(handle) = self.server_thread.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_artifact_store(label: &str) -> ArtifactStore {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("lab_otel_{}_{}", label, nanos));
        fs::create_dir_all(&root).expect("create artifact root");
        ArtifactStore::new(root)
    }

    #[test]
    fn otlp_receiver_stop_unblocks_without_requests() {
        let receiver = OtlpReceiver::start(0, temp_artifact_store("stop")).expect("start receiver");
        receiver.stop();
    }

    #[test]
    fn otlp_receiver_port_zero_allocates_ephemeral_port() {
        let receiver_a =
            OtlpReceiver::start(0, temp_artifact_store("port_a")).expect("start receiver a");
        let receiver_b =
            OtlpReceiver::start(0, temp_artifact_store("port_b")).expect("start receiver b");
        assert!(
            !receiver_a.endpoint.ends_with(":0"),
            "receiver endpoint should use an assigned port: {}",
            receiver_a.endpoint
        );
        assert_ne!(
            receiver_a.endpoint, receiver_b.endpoint,
            "port zero should allocate unique endpoints"
        );
        receiver_a.stop();
        receiver_b.stop();
    }
}
