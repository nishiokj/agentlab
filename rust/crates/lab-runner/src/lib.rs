mod types;
pub use types::*;

mod config;
use config::*;
pub use config::validate_knob_overrides;

mod persistence;
mod sink;

use persistence::sqlite_store::{
    run_sqlite_path, JsonRowTable, SqliteRunStore as BackingSqliteStore,
};
use sink::{
    EventRow, MetricRow, RunManifestRecord, RunSink, SqliteRunStore, TrialRecord,
    VariantSnapshotRow,
};

// Core types, constants, adapter traits, leases, and entrypoint wrappers.
include!("core.rs");
// Continue/recover/replay/fork/pause/kill lifecycle operations.
include!("runner.rs");
// Schedule engine, execution coordinator, worker plumbing, and packaging.
include!("lifecycle.rs");
// Preflight checks, policy loading, benchmark/task model config.
include!("validations.rs");
// Runtime IO wiring, task boundary/workspace materialization, adapter process IO.
include!("io.rs");
// Runner test suite.
include!("tests.rs");
