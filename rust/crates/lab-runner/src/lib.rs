mod backend;
mod config;
mod experiment;
mod model;
mod package;
mod persistence;
mod trial;
mod util;

/// Global flag set by the ctrlc handler to request graceful shutdown.
/// Checked by the schedule engine between trials.
pub static INTERRUPTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub use experiment::control::{
    kill_run, pause_run, resume_trial, KillResult, PauseResult, ResumeMode, ResumeResult,
};
pub use experiment::preflight::{preflight_experiment, preflight_experiment_with_options};
pub use experiment::runner::{
    continue_run, continue_run_with_options, describe_experiment, describe_experiment_with_options,
    fork_trial, recover_run, replay_trial, run_experiment, run_experiment_strict,
    run_experiment_strict_with_options, run_experiment_with_options,
};
pub use experiment::state::RunExecutionOptions;
pub use model::{
    BuildResult, ExperimentSummary, ForkResult, MaterializationMode, PreflightCheck,
    PreflightReport, PreflightSeverity, RecoverResult, ReplayResult, RunResult,
};
pub use package::compile::build_experiment_package;
pub use package::validate::validate_knob_overrides;

// Runner test suite.
#[cfg(test)]
include!("tests.rs");
