export type JsonMap = Record<string, unknown>;

export interface LabErrorPayload {
  code: string;
  message: string;
  details?: unknown;
}

export interface LabErrorEnvelope {
  ok: false;
  error: LabErrorPayload;
}

export interface ExperimentSummary {
  experiment: string;
  workload_type: string;
  dataset: string;
  tasks: number;
  replications: number;
  variant_plan_entries: number;
  total_trials: number;
  harness: string[];
  integration_level: string;
  container_mode: boolean;
  image?: string | null;
  network: string;
  events_path?: string | null;
  tracing?: string | null;
  control_path: string;
  harness_script_resolved?: string | null;
  harness_script_exists: boolean;
}

export interface RunResult {
  run_id: string;
  run_dir: string;
}

export interface DescribeResponse {
  ok: true;
  command: 'describe';
  summary: ExperimentSummary;
}

export interface RunResponse {
  ok: true;
  command: 'run' | 'run-dev' | 'run-experiment';
  summary: ExperimentSummary;
  run: RunResult;
  container?: boolean;
  dev_setup?: string | null;
  dev_network_mode?: string;
  experiment_network_requirement?: string;
}

export interface PublishResponse {
  ok: true;
  command: 'publish';
  bundle: string;
  run_dir: string;
}

export interface ReplayResult {
  replay_id: string;
  replay_dir: string;
  parent_trial_id: string;
  strict: boolean;
  replay_grade: string;
  harness_status: string;
}

export interface ReplayResponse {
  ok: true;
  command: 'replay';
  replay: ReplayResult;
}

export interface ForkResult {
  fork_id: string;
  fork_dir: string;
  parent_trial_id: string;
  selector: string;
  strict: boolean;
  source_checkpoint: string | null;
  fallback_mode: string;
  replay_grade: string;
  harness_status: string;
}

export interface ForkResponse {
  ok: true;
  command: 'fork';
  fork: ForkResult;
}

export interface PauseResult {
  run_id: string;
  trial_id: string;
  label: string;
  checkpoint_acked: boolean;
  stop_acked: boolean;
}

export interface PauseResponse {
  ok: true;
  command: 'pause';
  pause: PauseResult;
}

export interface ResumeResult {
  trial_id: string;
  selector: string;
  fork: ForkResult;
}

export interface ResumeResponse {
  ok: true;
  command: 'resume';
  resume: ResumeResult;
}

export interface ValidateResponse {
  ok: true;
  command: 'knobs-validate' | 'schema-validate' | 'hooks-validate';
  valid: true;
  [key: string]: unknown;
}

export type JsonCommandResponse =
  | DescribeResponse
  | RunResponse
  | ReplayResponse
  | ForkResponse
  | PauseResponse
  | ResumeResponse
  | PublishResponse
  | ValidateResponse;

export interface CommandOptions {
  cwd?: string;
  env?: NodeJS.ProcessEnv;
}

export interface LabClientOptions {
  runnerBin?: string;
  cwd?: string;
  env?: NodeJS.ProcessEnv;
}

export interface DescribeArgs extends CommandOptions {
  experiment: string;
  overrides?: string;
}

export interface RunArgs extends DescribeArgs {
  container?: boolean;
}

export interface RunDevArgs extends DescribeArgs {
  setup?: string;
}

export interface RunExperimentArgs extends DescribeArgs {}

export interface ReplayArgs extends CommandOptions {
  runDir: string;
  trialId: string;
  strict?: boolean;
}

export interface ForkArgs extends CommandOptions {
  runDir: string;
  fromTrial: string;
  at: string;
  set?: JsonMap;
  strict?: boolean;
}

export interface PauseArgs extends CommandOptions {
  runDir: string;
  trialId?: string;
  label?: string;
  timeoutSeconds?: number;
}

export interface ResumeArgs extends CommandOptions {
  runDir: string;
  trialId?: string;
  label?: string;
  set?: JsonMap;
  strict?: boolean;
}

export interface PublishArgs extends CommandOptions {
  runDir: string;
  out?: string;
}

export interface KnobsValidateArgs extends CommandOptions {
  manifest: string;
  overrides: string;
}

export interface HooksValidateArgs extends CommandOptions {
  manifest: string;
  events: string;
}

export interface SchemaValidateArgs extends CommandOptions {
  schema: string;
  file: string;
}
