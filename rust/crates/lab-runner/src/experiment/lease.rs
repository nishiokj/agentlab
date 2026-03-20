use crate::config::atomic_write_json_pretty;
use crate::model::{
    ENGINE_LEASE_HEARTBEAT_SECONDS, ENGINE_LEASE_TTL_SECONDS, OPERATION_LEASE_TTL_SECONDS,
    RUNTIME_KEY_ENGINE_LEASE,
};
use crate::persistence::store::SqliteRunStore;

use anyhow::{anyhow, Result};
use chrono::Utc;
use lab_core::{ensure_dir, sha256_bytes};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub(crate) enum RunOperationType {
    Continue,
    Recover,
    Pause,
    Kill,
    Resume,
    Fork,
    Replay,
}

impl RunOperationType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Recover => "recover",
            Self::Pause => "pause",
            Self::Kill => "kill",
            Self::Resume => "resume",
            Self::Fork => "fork",
            Self::Replay => "replay",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OperationLeaseRecord {
    pub(crate) schema_version: String,
    pub(crate) operation_id: String,
    pub(crate) op_type: String,
    pub(crate) owner_pid: u32,
    pub(crate) owner_host: String,
    pub(crate) acquired_at: String,
    pub(crate) expires_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) stale_takeover_of: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EngineLeaseRecord {
    pub(crate) schema_version: String,
    pub(crate) run_id: String,
    pub(crate) owner_id: String,
    pub(crate) pid: u32,
    pub(crate) hostname: String,
    pub(crate) started_at: String,
    pub(crate) heartbeat_at: String,
    pub(crate) expires_at: String,
    pub(crate) epoch: u64,
}

#[derive(Debug)]
pub(crate) struct RunOperationLease {
    path: PathBuf,
    operation_id: String,
}

impl Drop for RunOperationLease {
    fn drop(&mut self) {
        let should_remove = fs::read(&self.path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<OperationLeaseRecord>(&bytes).ok())
            .map(|record| record.operation_id == self.operation_id)
            .unwrap_or(false);
        if should_remove {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub(crate) struct EngineLeaseGuard {
    stop: Arc<AtomicBool>,
    join_handle: Option<thread::JoinHandle<()>>,
}

impl Drop for EngineLeaseGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}

pub(crate) fn operation_lease_path(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime").join("operation_lease.json")
}

pub(crate) fn operation_owner_host() -> String {
    env::var("HOSTNAME")
        .or_else(|_| env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown-host".to_string())
}

pub(crate) fn parse_rfc3339_utc(raw: &str) -> Option<chrono::DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|ts| ts.with_timezone(&Utc))
}

pub(crate) fn operation_lease_is_stale(
    record: &OperationLeaseRecord,
    now: chrono::DateTime<Utc>,
) -> bool {
    parse_rfc3339_utc(&record.expires_at)
        .map(|expires_at| now > expires_at)
        .unwrap_or(true)
}

pub(crate) fn engine_lease_is_stale(
    record: &EngineLeaseRecord,
    now: chrono::DateTime<Utc>,
) -> bool {
    parse_rfc3339_utc(&record.expires_at)
        .map(|expires_at| now > expires_at)
        .unwrap_or(true)
}

fn next_unique_id(prefix: &str) -> String {
    static UNIQUE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
    let nonce = UNIQUE_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let ts = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let raw = format!(
        "{}:{}:{}:{}:{}",
        prefix,
        ts,
        std::process::id(),
        nonce,
        operation_owner_host()
    );
    let digest = sha256_bytes(raw.as_bytes());
    format!("{}_{}", prefix, &digest[..16])
}

pub(crate) fn make_operation_lease_record(
    op_type: RunOperationType,
    stale_takeover_of: Option<String>,
) -> OperationLeaseRecord {
    let now = Utc::now();
    let expires = now + chrono::Duration::seconds(OPERATION_LEASE_TTL_SECONDS);
    OperationLeaseRecord {
        schema_version: "operation_lease_v1".to_string(),
        operation_id: next_unique_id("op"),
        op_type: op_type.as_str().to_string(),
        owner_pid: std::process::id(),
        owner_host: operation_owner_host(),
        acquired_at: now.to_rfc3339(),
        expires_at: expires.to_rfc3339(),
        stale_takeover_of,
    }
}

pub(crate) fn acquire_run_operation_lease(
    run_dir: &Path,
    op_type: RunOperationType,
) -> Result<RunOperationLease> {
    let lease_path = operation_lease_path(run_dir);
    if let Some(parent) = lease_path.parent() {
        ensure_dir(parent)?;
    }
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lease_path)
    {
        Ok(mut file) => {
            let lease = make_operation_lease_record(op_type, None);
            let bytes = serde_json::to_vec_pretty(&lease)?;
            file.write_all(&bytes)?;
            file.write_all(b"\n")?;
            let _ = file.sync_all();
            if let Some(parent) = lease_path.parent() {
                if let Ok(dir) = fs::File::open(parent) {
                    let _ = dir.sync_all();
                }
            }
            Ok(RunOperationLease {
                path: lease_path,
                operation_id: lease.operation_id,
            })
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let now = Utc::now();
            let existing = fs::read(&lease_path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<OperationLeaseRecord>(&bytes).ok());
            if let Some(existing) = existing {
                if operation_lease_is_stale(&existing, now) {
                    let replacement =
                        make_operation_lease_record(op_type, Some(existing.operation_id.clone()));
                    atomic_write_json_pretty(&lease_path, &serde_json::to_value(&replacement)?)?;
                    return Ok(RunOperationLease {
                        path: lease_path,
                        operation_id: replacement.operation_id,
                    });
                }
            }
            Err(anyhow!(
                "operation_in_progress: run is already under control operation"
            ))
        }
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn load_engine_lease(run_dir: &Path) -> Result<Option<EngineLeaseRecord>> {
    let store = SqliteRunStore::open(run_dir)?;
    let Some(value) = store.get_runtime_json(RUNTIME_KEY_ENGINE_LEASE)? else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_value::<EngineLeaseRecord>(value)?))
}

pub(crate) fn write_engine_lease(run_dir: &Path, lease: &EngineLeaseRecord) -> Result<()> {
    let mut store = SqliteRunStore::open(run_dir)?;
    store.put_runtime_json(RUNTIME_KEY_ENGINE_LEASE, &serde_json::to_value(lease)?)
}

pub(crate) fn make_engine_lease(run_id: &str, epoch: u64) -> EngineLeaseRecord {
    let now = Utc::now();
    let expires = now + chrono::Duration::seconds(ENGINE_LEASE_TTL_SECONDS);
    EngineLeaseRecord {
        schema_version: "engine_lease_v1".to_string(),
        run_id: run_id.to_string(),
        owner_id: next_unique_id("owner"),
        pid: std::process::id(),
        hostname: operation_owner_host(),
        started_at: now.to_rfc3339(),
        heartbeat_at: now.to_rfc3339(),
        expires_at: expires.to_rfc3339(),
        epoch,
    }
}

pub(crate) fn ensure_engine_lease_for_run(
    run_dir: &Path,
    run_id: &str,
) -> Result<EngineLeaseRecord> {
    if let Some(existing) = load_engine_lease(run_dir)? {
        if existing.run_id != run_id {
            return Err(anyhow!(
                "engine lease run_id mismatch: lease has {}, run expects {}",
                existing.run_id,
                run_id
            ));
        }
        return Ok(existing);
    }
    let lease = make_engine_lease(run_id, 1);
    write_engine_lease(run_dir, &lease)?;
    Ok(lease)
}

pub(crate) fn start_engine_lease_heartbeat(
    run_dir: &Path,
    run_id: &str,
) -> Result<EngineLeaseGuard> {
    let existing = ensure_engine_lease_for_run(run_dir, run_id)?;
    let mut heartbeat_lease = make_engine_lease(run_id, existing.epoch + 1);
    write_engine_lease(run_dir, &heartbeat_lease)?;
    let run_dir = run_dir.to_path_buf();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_signal = stop.clone();
    let join_handle = thread::spawn(move || {
        while !stop_signal.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_secs(ENGINE_LEASE_HEARTBEAT_SECONDS as u64));
            let now = Utc::now();
            heartbeat_lease.heartbeat_at = now.to_rfc3339();
            heartbeat_lease.expires_at =
                (now + chrono::Duration::seconds(ENGINE_LEASE_TTL_SECONDS)).to_rfc3339();
            let _ = write_engine_lease(&run_dir, &heartbeat_lease);
        }
    });
    Ok(EngineLeaseGuard {
        stop,
        join_handle: Some(join_handle),
    })
}

pub(crate) fn adopt_engine_lease_for_recovery(
    run_dir: &Path,
    run_id: &str,
    force: bool,
) -> Result<EngineLeaseRecord> {
    let now = Utc::now();
    let existing = load_engine_lease(run_dir)?;
    if let Some(ref lease) = existing {
        if lease.run_id != run_id {
            return Err(anyhow!(
                "engine lease run_id mismatch: lease has {}, run expects {}",
                lease.run_id,
                run_id
            ));
        }
        if !force && !engine_lease_is_stale(lease, now) {
            return Err(anyhow!(
                "run_owner_alive: engine owner '{}' pid={} lease expires {}",
                lease.owner_id,
                lease.pid,
                lease.expires_at
            ));
        }
    }
    let epoch = existing.map(|lease| lease.epoch + 1).unwrap_or(1);
    let adopted = make_engine_lease(run_id, epoch);
    write_engine_lease(run_dir, &adopted)?;
    Ok(adopted)
}
