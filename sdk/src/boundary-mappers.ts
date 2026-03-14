import type { HookEvent } from './hook-events.js';
import type { TrialOutput } from './trial-output.js';

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

// ---------------------------------------------------------------------------
// Standardized runner contracts (v1)
// ---------------------------------------------------------------------------

export interface WorkspaceContractV1 {
  root: '/agentlab/workspace';
  task_manifest_path: '/agentlab/workspace/.agentlab/task-manifest.json';
  artifacts_dir: '/agentlab/workspace/.agentlab/artifacts';
}

export interface InvocationEnvContractV1 {
  control_path: 'AGENTLAB_CONTROL_PATH';
  control_mode: 'AGENTLAB_CONTROL_MODE';
  harness_root: 'AGENTLAB_HARNESS_ROOT';
}

export interface InvocationContractV1 {
  command: string[];
  env: InvocationEnvContractV1;
}

export interface MountSemanticsContractV1 {
  dataset_pack_ref_format: 'sha256:<hex64>';
  read_only: true;
}

export interface RunnerBoundaryManifestV1 {
  schema_version: 'runner_boundary_manifest_v1';
  workspace: WorkspaceContractV1;
  mount_semantics: MountSemanticsContractV1;
  invocation: InvocationContractV1;
}

export const WORKSPACE_CONTRACT_V1: WorkspaceContractV1 = {
  root: '/agentlab/workspace',
  task_manifest_path: '/agentlab/workspace/.agentlab/task-manifest.json',
  artifacts_dir: '/agentlab/workspace/.agentlab/artifacts',
};

export const INVOCATION_ENV_CONTRACT_V1: InvocationEnvContractV1 = {
  control_path: 'AGENTLAB_CONTROL_PATH',
  control_mode: 'AGENTLAB_CONTROL_MODE',
  harness_root: 'AGENTLAB_HARNESS_ROOT',
};

const MOUNT_SEMANTICS_CONTRACT_V1: MountSemanticsContractV1 = {
  dataset_pack_ref_format: 'sha256:<hex64>',
  read_only: true,
};

export function createRunnerBoundaryManifest(
  command: readonly string[],
): RunnerBoundaryManifestV1 {
  if (command.length === 0) {
    throw new Error('invocation command must have at least one token');
  }
  for (const token of command) {
    if (!token.trim()) {
      throw new Error('invocation command tokens must be non-empty');
    }
  }
  return {
    schema_version: 'runner_boundary_manifest_v1',
    workspace: { ...WORKSPACE_CONTRACT_V1 },
    mount_semantics: { ...MOUNT_SEMANTICS_CONTRACT_V1 },
    invocation: {
      command: [...command],
      env: { ...INVOCATION_ENV_CONTRACT_V1 },
    },
  };
}

// ---------------------------------------------------------------------------
// Input boundary (user-implemented mapper target)
// All benchmark inputs must compile into:
//   task + environment + workspace + limits
// ---------------------------------------------------------------------------

export interface WorkspaceOverlayV3 {
  path: string;
  content: string;
  encoding?: 'utf8' | 'base64';
  executable?: boolean;
}

export interface WorkspaceAuxMountV3 {
  dataset_pack_ref: string;
  mount_path: string;
}

export interface TaskLimitsV1 {
  max_steps?: number;
  max_total_tokens?: number;
  max_tool_calls?: number;
  trial_seconds?: number;
}

export interface TaskEnvironmentV3 {
  image: string;
}

export interface WorkspaceBaseEmptyV3 {
  kind: 'empty';
}

export interface WorkspaceBaseDatasetPackV3 {
  kind: 'dataset_pack';
  dataset_pack_ref: string;
}

export interface WorkspaceBaseGitCheckoutV3 {
  kind: 'git_checkout';
  repo: string;
  commit: string;
}

export type WorkspaceBaseV3 =
  | WorkspaceBaseEmptyV3
  | WorkspaceBaseDatasetPackV3
  | WorkspaceBaseGitCheckoutV3;

export interface WorkspaceSpecV3 {
  mode: 'scratch' | 'patch';
  base: WorkspaceBaseV3;
  overlays: WorkspaceOverlayV3[];
  aux_mounts: WorkspaceAuxMountV3[];
}

export interface TaskSpecV1 {
  schema_version: 'task_spec_v1';
  task: Record<string, JsonValue>;
  environment: TaskEnvironmentV3;
  workspace: WorkspaceSpecV3;
  dependencies?: Record<string, JsonValue>;
  limits: TaskLimitsV1;
}

export interface InputMapperContext {
  index: number;
}

export interface InputMapper<TInput> {
  map(input: TInput, context: InputMapperContext): TaskSpecV1;
}

const TASK_BOUNDARY_KEYS = new Set([
  'schema_version',
  'task',
  'environment',
  'workspace',
  'dependencies',
  'limits',
]);

const DATASET_PACK_REF_RE = /^sha256:[0-9a-f]{64}$/;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value);
}

function isJsonValue(value: unknown): value is JsonValue {
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  ) {
    return true;
  }
  if (Array.isArray(value)) {
    return value.every((item) => isJsonValue(item));
  }
  if (isPlainObject(value)) {
    return Object.values(value).every((item) => isJsonValue(item));
  }
  return false;
}

function assertPositiveInt(
  value: number | undefined,
  fieldName: keyof TaskLimitsV1,
): void {
  if (value === undefined) {
    return;
  }
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${fieldName} must be a positive integer when provided`);
  }
}

function readOptionalNumber(
  obj: Record<string, unknown>,
  fieldName: keyof TaskLimitsV1,
): number | undefined {
  const value = obj[fieldName];
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== 'number') {
    throw new Error(`${fieldName} must be a number when provided`);
  }
  return value;
}

function assertWorkspaceRelativePath(path: unknown, field: string): void {
  if (typeof path !== 'string' || !path.trim()) {
    throw new Error(`${field} must be a non-empty string`);
  }
  if (path.startsWith('/')) {
    throw new Error(`${field} must be relative to /agentlab/workspace`);
  }
  const segments = path.split('/').filter((segment) => segment.length > 0);
  if (segments.length === 0 || segments.includes('..')) {
    throw new Error(`${field} must stay within /agentlab/workspace`);
  }
}

function assertWorkspaceBase(base: unknown): void {
  if (!isPlainObject(base)) {
    throw new Error('task boundary workspace.base must be an object');
  }
  if (typeof base.kind !== 'string' || !base.kind.trim()) {
    throw new Error('task boundary workspace.base.kind must be a non-empty string');
  }

  const keys = new Set(Object.keys(base));
  switch (base.kind) {
    case 'empty': {
      if (keys.size !== 1) {
        throw new Error('workspace.base kind "empty" does not accept extra fields');
      }
      return;
    }
    case 'dataset_pack': {
      if (
        keys.size !== 2 ||
        typeof base.dataset_pack_ref !== 'string' ||
        !DATASET_PACK_REF_RE.test(base.dataset_pack_ref)
      ) {
        throw new Error(
          'workspace.base kind "dataset_pack" requires dataset_pack_ref matching sha256:<hex64>',
        );
      }
      return;
    }
    case 'git_checkout': {
      if (
        keys.size !== 3 ||
        typeof base.repo !== 'string' ||
        !base.repo.trim() ||
        typeof base.commit !== 'string' ||
        !base.commit.trim()
      ) {
        throw new Error(
          'workspace.base kind "git_checkout" requires non-empty repo and commit',
        );
      }
      return;
    }
    default:
      throw new Error(
        'task boundary workspace.base.kind must be one of: empty, dataset_pack, git_checkout',
      );
  }
}

export function assertTaskSpecV1(boundary: unknown): asserts boundary is TaskSpecV1 {
  if (!isPlainObject(boundary)) {
    throw new Error('task boundary must be an object');
  }

  const keys = Object.keys(boundary);
  for (const key of keys) {
    if (!TASK_BOUNDARY_KEYS.has(key)) {
      throw new Error(
        `task boundary contains unsupported key "${key}". ` +
          'Boundary must compile into exactly: task + environment + workspace + limits',
      );
    }
  }

  if (boundary.schema_version !== 'task_spec_v1') {
    throw new Error('task spec schema_version must be "task_spec_v1"');
  }

  if (!isPlainObject(boundary.task)) {
    throw new Error('task boundary task must be an object');
  }
  if ('image' in boundary.task) {
    throw new Error('task.image is removed; use environment.image');
  }
  if ('workspace' in boundary.task) {
    throw new Error('task.workspace is removed; sandbox topology is runner-owned');
  }
  for (const [key, value] of Object.entries(boundary.task)) {
    if (!isJsonValue(value)) {
      throw new Error(`task field "${key}" is not valid JSON`);
    }
  }

  if (!isPlainObject(boundary.environment)) {
    throw new Error('task boundary environment must be an object');
  }
  if (typeof boundary.environment.image !== 'string' || !boundary.environment.image.trim()) {
    throw new Error('task boundary environment.image must be a non-empty string');
  }
  if (Object.keys(boundary.environment).length !== 1) {
    throw new Error('task boundary environment only supports the image field');
  }

  if (!isPlainObject(boundary.workspace)) {
    throw new Error('task boundary workspace must be an object');
  }
  if (
    boundary.workspace.mode !== 'scratch' &&
    boundary.workspace.mode !== 'patch'
  ) {
    throw new Error('task boundary workspace.mode must be "scratch" or "patch"');
  }
  assertWorkspaceBase(boundary.workspace.base);

  if (!Array.isArray(boundary.workspace.overlays)) {
    throw new Error('task boundary workspace.overlays must be an array');
  }
  for (const [index, file] of boundary.workspace.overlays.entries()) {
    if (!isPlainObject(file)) {
      throw new Error(`workspace.overlays[${index}] must be an object`);
    }
    assertWorkspaceRelativePath(file.path, `workspace.overlays[${index}].path`);
    if (typeof file.content !== 'string') {
      throw new Error(`workspace.overlays[${index}].content must be a string`);
    }
    if (
      file.encoding !== undefined &&
      file.encoding !== 'utf8' &&
      file.encoding !== 'base64'
    ) {
      throw new Error(`workspace.overlays[${index}].encoding must be "utf8" or "base64"`);
    }
    if (file.executable !== undefined && typeof file.executable !== 'boolean') {
      throw new Error(
        `workspace.overlays[${index}].executable must be a boolean when provided`,
      );
    }
  }

  if (!Array.isArray(boundary.workspace.aux_mounts)) {
    throw new Error('task boundary workspace.aux_mounts must be an array');
  }
  for (const [index, mount] of boundary.workspace.aux_mounts.entries()) {
    if (!isPlainObject(mount)) {
      throw new Error(`workspace.aux_mounts[${index}] must be an object`);
    }
    if (typeof mount.mount_path !== 'string' || !mount.mount_path.trim()) {
      throw new Error(`workspace.aux_mounts[${index}].mount_path must be a non-empty string`);
    }
    if (!mount.mount_path.startsWith('/agentlab/workspace')) {
      throw new Error(
        `workspace.aux_mounts[${index}].mount_path must target /agentlab/workspace`,
      );
    }
    if (
      typeof mount.dataset_pack_ref !== 'string' ||
      !DATASET_PACK_REF_RE.test(mount.dataset_pack_ref)
    ) {
      throw new Error(
        `workspace.aux_mounts[${index}].dataset_pack_ref must match sha256:<hex64>`,
      );
    }
  }

  if (
    boundary.workspace.mode === 'patch' &&
    isPlainObject(boundary.workspace.base) &&
    boundary.workspace.base.kind === 'empty'
  ) {
    throw new Error('patch tasks require a real workspace.base and may not use kind "empty"');
  }

  if (!isPlainObject(boundary.limits)) {
    throw new Error('task boundary limits must be an object');
  }
  const limits = boundary.limits;
  assertPositiveInt(readOptionalNumber(limits, 'max_steps'), 'max_steps');
  assertPositiveInt(
    readOptionalNumber(limits, 'max_total_tokens'),
    'max_total_tokens',
  );
  assertPositiveInt(readOptionalNumber(limits, 'max_tool_calls'), 'max_tool_calls');
  assertPositiveInt(readOptionalNumber(limits, 'trial_seconds'), 'trial_seconds');
}

export function compileTaskSpecs<TInput>(
  inputs: readonly TInput[],
  mapper: InputMapper<TInput>,
: TaskSpecV1[] {
  return inputs.map((input, index) => {
    const boundary = mapper.map(input, { index });
    assertTaskSpecV1(boundary);
    return boundary;
  });
}

export function taskSpecsToJsonl(boundaries: readonly TaskSpecV1[]): string {
  const lines = boundaries.map((boundary) => {
    assertTaskSpecV1(boundary);
    return JSON.stringify(boundary);
  });
  if (lines.length === 0) {
    return '';
  }
  return `${lines.join('\n')}\n`;
}

// ---------------------------------------------------------------------------
// Outcome boundary (runner-emitted boundary -> user mapper)
// ---------------------------------------------------------------------------

export interface OutcomeResultSummaryV1 {
  ids: TrialOutput['ids'];
  outcome: TrialOutput['outcome'];
  metrics?: TrialOutput['metrics'];
  objective?: TrialOutput['objective'];
  artifacts?: TrialOutput['artifacts'];
  checkpoints?: TrialOutput['checkpoints'];
  error?: TrialOutput['error'];
  ext?: TrialOutput['ext'];
}

export interface OutcomeBoundaryV1 {
  schema_version: 'outcome_boundary_v1';
  run_events: HookEvent[];
  result_summary: OutcomeResultSummaryV1;
}

export interface OutcomeMapper<TMappedOutcome> {
  map(boundary: OutcomeBoundaryV1): TMappedOutcome | Promise<TMappedOutcome>;
}

export function createOutcomeBoundary(
  trialOutput: TrialOutput,
  runEvents: readonly HookEvent[] = [],
): OutcomeBoundaryV1 {
  return {
    schema_version: 'outcome_boundary_v1',
    run_events: [...runEvents],
    result_summary: {
      ids: trialOutput.ids,
      outcome: trialOutput.outcome,
      metrics: trialOutput.metrics,
      objective: trialOutput.objective,
      artifacts: trialOutput.artifacts,
      checkpoints: trialOutput.checkpoints,
      error: trialOutput.error,
      ext: trialOutput.ext,
    },
  };
}

export async function mapOutcome<TMappedOutcome>(
  boundary: OutcomeBoundaryV1,
  mapper: OutcomeMapper<TMappedOutcome>,
): Promise<TMappedOutcome> {
  return mapper.map(boundary);
}
