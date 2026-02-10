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
  docker_build_status?: string | null;
}

export interface PublishResponse {
  ok: true;
  command: 'publish';
  bundle: string;
  run_dir: string;
}

export interface ValidateResponse {
  ok: true;
  command: 'knobs-validate' | 'schema-validate' | 'hooks-validate';
  valid: true;
  [key: string]: unknown;
}

export interface ImageBuildResponse {
  ok: true;
  command: 'image-build';
  image: string;
  dockerfile: string;
  context: string;
  docker_build: string;
}

export type JsonCommandResponse =
  | DescribeResponse
  | RunResponse
  | PublishResponse
  | ValidateResponse
  | ImageBuildResponse;

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

export interface RunExperimentArgs extends DescribeArgs {
  buildImage?: boolean;
  tag?: string;
}

export interface ImageBuildArgs extends DescribeArgs {
  tag?: string;
  dockerfile?: string;
  context?: string;
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
