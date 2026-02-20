import { stringify as yamlStringify } from 'yaml';

export type Bindings = Record<string, unknown>;

export interface DatasetJsonlOptions {
  suiteId: string;
  provider?: 'local_jsonl';
  schemaVersion?: string;
  splitId: string;
  limit: number;
}

export interface DependencyFileStagingEntry {
  source_from_host: string;
  destination_path: string;
  required?: boolean;
}

export interface DependencyAssetEntry {
  id?: string;
  source_from_host: string;
  mount_path: string;
  read_only?: boolean;
  required?: boolean;
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

export type MetricSource = 'runner' | 'events' | 'output' | 'artifacts';
export type ArtifactMeasure = 'file_count' | 'diff_bytes' | 'diff_lines' | 'total_bytes';
export type MetricAggregate = 'sum' | 'count' | 'max' | 'min' | 'mean' | 'last';

// ---------------------------------------------------------------------------
// Guardrails
// ---------------------------------------------------------------------------

export interface GuardrailDef {
  metric_id: string;
  max?: number;
}

export interface MetricDef {
  id: string;
  source: MetricSource;
  /** For source: 'output' — JSON pointer into result.json */
  json_pointer?: string;
  /** For source: 'events' — which hook event type to aggregate */
  event_type?: string;
  /** For source: 'events' — dot-path to the numeric field within the event */
  event_field?: string;
  /** For source: 'events' — how to aggregate across events in a trial */
  aggregate?: MetricAggregate;
  /** For source: 'artifacts' — what to measure from collected artifacts */
  artifact_measure?: ArtifactMeasure;
  /** For source: 'artifacts' — optional glob filter for the measurement */
  artifact_glob?: string;
  /** 0 = observe only (default). > 0 = contributes to composite score. */
  weight: number;
  /** Whether higher or lower is better. */
  direction?: 'maximize' | 'minimize';
  /** Primary metrics are highlighted in analysis summaries. */
  primary: boolean;
}

/**
 * Factory for metric definitions. Predefined constants for runner/event
 * auto-metrics, plus helpers for output-derived and custom event metrics.
 */
export class Metric {
  // -- Runner auto-metrics (always tracked, no agent involvement) ------------

  static readonly DURATION_MS: MetricDef = {
    id: 'duration_ms', source: 'runner', weight: 0, primary: false,
  };
  static readonly EXIT_CODE: MetricDef = {
    id: 'exit_code', source: 'runner', weight: 0, primary: false,
  };

  // -- Event auto-metrics (tracked when integrationLevel >= cli_events) ------

  static readonly TOKENS_IN: MetricDef = {
    id: 'tokens_in', source: 'events',
    event_type: 'model_call_end', event_field: 'usage.tokens_in', aggregate: 'sum',
    weight: 0, primary: false,
  };
  static readonly TOKENS_OUT: MetricDef = {
    id: 'tokens_out', source: 'events',
    event_type: 'model_call_end', event_field: 'usage.tokens_out', aggregate: 'sum',
    weight: 0, primary: false,
  };
  static readonly STEP_COUNT: MetricDef = {
    id: 'step_count', source: 'events',
    event_type: 'agent_step_start', aggregate: 'count',
    weight: 0, primary: false,
  };
  static readonly TURN_COUNT: MetricDef = {
    id: 'turn_count', source: 'events',
    event_type: 'model_call_end', aggregate: 'count',
    weight: 0, primary: false,
  };
  static readonly TOOL_CALL_COUNT: MetricDef = {
    id: 'tool_call_count', source: 'events',
    event_type: 'tool_call_end', aggregate: 'count',
    weight: 0, primary: false,
  };

  // -- Artifact auto-metrics (tracked when artifacts.diff is enabled) --------

  static readonly FILES_CREATED: MetricDef = {
    id: 'files_created', source: 'artifacts',
    artifact_measure: 'file_count', weight: 0, primary: false,
  };
  static readonly FILES_MODIFIED: MetricDef = {
    id: 'files_modified', source: 'artifacts',
    artifact_measure: 'file_count', weight: 0, primary: false,
  };
  static readonly DIFF_BYTES: MetricDef = {
    id: 'diff_bytes', source: 'artifacts',
    artifact_measure: 'diff_bytes', weight: 0, primary: false,
  };
  static readonly DIFF_LINES: MetricDef = {
    id: 'diff_lines', source: 'artifacts',
    artifact_measure: 'diff_lines', weight: 0, primary: false,
  };

  // -- Factories -------------------------------------------------------------

  /** Metric extracted from a field in result.json. */
  static fromOutput(id: string, jsonPointer: string, options?: {
    weight?: number;
    direction?: 'maximize' | 'minimize';
    primary?: boolean;
  }): MetricDef {
    return {
      id,
      source: 'output',
      json_pointer: jsonPointer,
      weight: options?.weight ?? 0,
      direction: options?.direction,
      primary: options?.primary ?? false,
    };
  }

  /** Metric computed by aggregating a field across hook events in a trial. */
  static fromEvents(id: string, options: {
    eventType: string;
    eventField?: string;
    aggregate: MetricAggregate;
    weight?: number;
    direction?: 'maximize' | 'minimize';
    primary?: boolean;
  }): MetricDef {
    return {
      id,
      source: 'events',
      event_type: options.eventType,
      event_field: options.eventField,
      aggregate: options.aggregate,
      weight: options?.weight ?? 0,
      direction: options?.direction,
      primary: options?.primary ?? false,
    };
  }

  /** Metric computed from workspace artifacts collected after a trial. */
  static fromArtifacts(id: string, options: {
    measure: ArtifactMeasure;
    glob?: string;
    weight?: number;
    direction?: 'maximize' | 'minimize';
    primary?: boolean;
  }): MetricDef {
    return {
      id,
      source: 'artifacts',
      artifact_measure: options.measure,
      artifact_glob: options.glob,
      weight: options?.weight ?? 0,
      direction: options?.direction,
      primary: options?.primary ?? false,
    };
  }

  // -- Guardrail factories ---------------------------------------------------

  static maxTokensIn(n: number): GuardrailDef {
    return { metric_id: 'tokens_in', max: n };
  }

  static maxTokensOut(n: number): GuardrailDef {
    return { metric_id: 'tokens_out', max: n };
  }

  static maxDuration(ms: number): GuardrailDef {
    return { metric_id: 'duration_ms', max: ms };
  }

  static maxToolCalls(n: number): GuardrailDef {
    return { metric_id: 'tool_call_count', max: n };
  }

  static maxTurns(n: number): GuardrailDef {
    return { metric_id: 'turn_count', max: n };
  }

  static maxCost(n: number): GuardrailDef {
    return { metric_id: 'cost_usd', max: n };
  }

  private constructor() {} // no instances
}

// ---------------------------------------------------------------------------
// Design Policies
// ---------------------------------------------------------------------------

export type SchedulingPolicy = 'paired_interleaved' | 'variant_sequential' | 'randomized';
export type ComparisonPolicy = 'paired' | 'unpaired' | 'none';

export type RetryTrigger = 'error' | 'timeout' | 'failure';

export interface RetryPolicy {
  max_attempts: number;
  retry_on?: readonly RetryTrigger[];
}

export interface PruningPolicy {
  max_consecutive_failures?: number;
}

export interface DesignPolicies {
  scheduling: SchedulingPolicy;
  comparison: ComparisonPolicy;
  retry: RetryPolicy;
  pruning?: PruningPolicy;
}

export type BenchmarkTaskModel = 'independent' | 'dependent';
export type BenchmarkScoringLifecycle = 'predict_then_score' | 'integrated_score';

export interface BenchmarkTypePolicy {
  task_model?: BenchmarkTaskModel;
  reset_strategy?: 'per_trial' | 'per_chain' | 'never';
  evaluator_mode?: 'official' | 'custom';
  scoring_lifecycle?: BenchmarkScoringLifecycle;
  required_evidence_classes?: string[];
  chain_failure_policy?: 'stop_on_error' | 'continue_with_flag';
}

export interface BenchmarkAdapterConfig {
  command: string[];
  manifest?: Record<string, unknown>;
}

export interface BenchmarkConfig {
  policy?: BenchmarkTypePolicy;
  adapter?: BenchmarkAdapterConfig;
}

export type AgentRuntimeMode = 'known_agent_ref' | 'custom_image';
export interface AgentAdapterRef {
  id: string;
  version: string;
}

export const BUILTIN_COMMAND_ADAPTER: AgentAdapterRef = {
  id: 'builtin.command_contract',
  version: 'v1',
};

export const PREBUILT_CODEX_ADAPTER: AgentAdapterRef = {
  id: 'prebuilt.codex_cli',
  version: 'v1',
};

export const PREBUILT_REX_JESUS_ADAPTER: AgentAdapterRef = {
  id: 'prebuilt.rex_jesus',
  version: 'v1',
};

export interface KnownAgentRef {
  id: string;
  version: string;
  registry?: string;
}

export interface CustomAgentImage {
  image?: string;
  entrypoint: string[];
}

export interface AgentRuntimeOverrides {
  args?: string[];
  env?: Record<string, string>;
  env_from_host?: string[];
}

function copyPolicies(p: DesignPolicies): DesignPolicies {
  return {
    scheduling: p.scheduling,
    comparison: p.comparison,
    retry: {
      max_attempts: p.retry.max_attempts,
      retry_on: p.retry.retry_on ? [...p.retry.retry_on] : undefined,
    },
    pruning: p.pruning ? { ...p.pruning } : undefined,
  };
}

// ---------------------------------------------------------------------------
// Experiment Type Presets
// ---------------------------------------------------------------------------

const DEFAULT_RETRY: RetryPolicy = { max_attempts: 1 };

export const ExperimentType = {
  AB_TEST: {
    scheduling: 'paired_interleaved',
    comparison: 'paired',
    retry: { ...DEFAULT_RETRY },
  } satisfies DesignPolicies,

  MULTI_VARIANT: {
    scheduling: 'paired_interleaved',
    comparison: 'paired',
    retry: { ...DEFAULT_RETRY },
  } satisfies DesignPolicies,

  PARAMETER_SWEEP: {
    scheduling: 'variant_sequential',
    comparison: 'unpaired',
    retry: { ...DEFAULT_RETRY },
  } satisfies DesignPolicies,

  REGRESSION: {
    scheduling: 'variant_sequential',
    comparison: 'none',
    retry: { max_attempts: 3, retry_on: ['error'] },
  } satisfies DesignPolicies,
} as const;

// ---------------------------------------------------------------------------
// ExperimentSpec
// ---------------------------------------------------------------------------

export interface ExperimentSpec {
  version: '0.5';
  experiment: {
    id: string;
    name: string;
    description?: string;
    owner?: string;
    tags?: string[];
  };
  dataset: {
    suite_id: string;
    provider: 'local_jsonl';
    path: string;
    schema_version: string;
    split_id: string;
    limit: number;
  };
  design: {
    sanitization_profile: string;
    comparison: ComparisonPolicy;
    replications: number;
    random_seed: number;
    shuffle_tasks: boolean;
    max_concurrency: number;
    policies?: DesignPolicies;
  };
  metrics: MetricDef[];
  guardrails?: GuardrailDef[];
  artifacts?: {
    /** Glob patterns for files to collect from workspace post-trial */
    collect: string[];
    /** Compute workspace diff (pre vs post trial snapshot) */
    diff: boolean;
    /** Base directory for collection, relative to workspace root */
    base_dir?: string;
  };
  baseline: {
    variant_id: string;
    bindings: Bindings;
  };
  variant_plan: Array<{
    variant_id: string;
    bindings: Bindings;
  }>;
  benchmark?: {
    policy?: BenchmarkTypePolicy;
    adapter?: BenchmarkAdapterConfig;
  };
  runtime: {
    agent: {
      mode: AgentRuntimeMode;
      adapter?: AgentAdapterRef;
      known_agent_ref?: KnownAgentRef;
      custom_image?: CustomAgentImage;
      overrides?: AgentRuntimeOverrides;
    };
    dependencies: {
      assets?: DependencyAssetEntry[];
      file_staging?: DependencyFileStagingEntry[];
      services?: Array<{
        id: string;
        kind: string;
        path: string;
      }>;
    };
    policy: {
      timeout_ms: number;
      network: {
        mode: 'none' | 'full' | 'allowlist_enforced';
        allowed_hosts: string[];
      };
      sandbox: {
        mode: 'container' | 'local';
        engine?: 'docker';
        image?: string;
        root_read_only?: boolean;
        run_as_user?: string;
        hardening?: {
          no_new_privileges: boolean;
          drop_all_caps: boolean;
        };
        resources?: {
          cpu_count: number;
          memory_mb: number;
        };
      };
    };
    telemetry?: {
      trajectory_path?: string;
      causal_extraction?: string;
    };
  };
  validity: {
    fail_on_state_leak: boolean;
    fail_on_profile_invariant_violation: boolean;
  };
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

export class ExperimentBuilder {
  private readonly spec: ExperimentSpec;

  /** Create a builder with explicit id/name (defaults to variant_sequential). */
  static create(id: string, name: string): ExperimentBuilder {
    return new ExperimentBuilder(id, name);
  }

  /** Create a builder from a policy bundle (preset or custom). Set id/name via .id()/.name(). */
  static from(policies: DesignPolicies): ExperimentBuilder {
    const builder = new ExperimentBuilder('', '');
    builder.spec.design.policies = copyPolicies(policies);
    builder.spec.design.comparison = policies.comparison;
    return builder;
  }

  private constructor(id: string, name: string) {
    this.spec = {
      version: '0.5',
      experiment: {
        id,
        name,
        tags: [],
      },
      dataset: {
        suite_id: '',
        provider: 'local_jsonl',
        path: '',
        schema_version: 'task_jsonl_v1',
        split_id: '',
        limit: 0,
      },
      design: {
        sanitization_profile: 'hermetic_functional',
        comparison: 'paired',
        replications: 1,
        random_seed: 1,
        shuffle_tasks: true,
        max_concurrency: 1,
      },
      metrics: [],
      baseline: {
        variant_id: 'base',
        bindings: {},
      },
      variant_plan: [],
      runtime: {
        agent: {
          mode: 'custom_image',
          custom_image: {
            entrypoint: [],
          },
          overrides: {
            args: [],
            env: {},
            env_from_host: [],
          },
        },
        dependencies: {
          assets: [],
          file_staging: [],
          services: [],
        },
        policy: {
          timeout_ms: 600_000,
          sandbox: { mode: 'local' },
          network: {
            mode: 'none',
            allowed_hosts: [],
          },
        },
      },
      validity: {
        fail_on_state_leak: true,
        fail_on_profile_invariant_violation: true,
      },
    };
  }

  id(value: string): this {
    this.spec.experiment.id = value;
    return this;
  }

  name(value: string): this {
    this.spec.experiment.name = value;
    return this;
  }

  description(value: string): this {
    this.spec.experiment.description = value;
    return this;
  }

  owner(value: string): this {
    this.spec.experiment.owner = value;
    return this;
  }

  tags(values: string[]): this {
    this.spec.experiment.tags = [...values];
    return this;
  }

  datasetJsonl(path: string, options: DatasetJsonlOptions): this {
    this.spec.dataset.path = path;
    this.spec.dataset.suite_id = options.suiteId;
    this.spec.dataset.provider = options.provider ?? 'local_jsonl';
    this.spec.dataset.schema_version = options.schemaVersion ?? this.spec.dataset.schema_version;
    this.spec.dataset.split_id = options.splitId;
    this.spec.dataset.limit = options.limit;
    return this;
  }

  private ensureAgentOverrides(): AgentRuntimeOverrides {
    if (!this.spec.runtime.agent.overrides) {
      this.spec.runtime.agent.overrides = {};
    }
    return this.spec.runtime.agent.overrides;
  }

  private ensureCustomImage(): CustomAgentImage {
    if (!this.spec.runtime.agent.custom_image) {
      this.spec.runtime.agent.custom_image = { entrypoint: [] };
    }
    return this.spec.runtime.agent.custom_image;
  }

  agentAdapter(id: string, version = 'v1'): this {
    this.spec.runtime.agent.adapter = { id, version };
    return this;
  }

  useBuiltinAdapter(version = 'v1'): this {
    return this.agentAdapter(BUILTIN_COMMAND_ADAPTER.id, version);
  }

  usePrebuiltCodexAdapter(version = 'v1'): this {
    return this.agentAdapter(PREBUILT_CODEX_ADAPTER.id, version);
  }

  usePrebuiltRexJesusAdapter(version = 'v1'): this {
    return this.agentAdapter(PREBUILT_REX_JESUS_ADAPTER.id, version);
  }

  agentRef(id: string, version: string, options?: { registry?: string }): this {
    this.spec.runtime.agent.mode = 'known_agent_ref';
    this.spec.runtime.agent.known_agent_ref = {
      id,
      version,
      registry: options?.registry,
    };
    this.spec.runtime.agent.custom_image = undefined;
    return this;
  }

  customAgentImage(image: string, entrypoint: string[]): this {
    this.spec.runtime.agent.mode = 'custom_image';
    this.spec.runtime.agent.known_agent_ref = undefined;
    this.spec.runtime.agent.custom_image = {
      image,
      entrypoint: [...entrypoint],
    };
    this.spec.runtime.policy.sandbox.mode = 'container';
    this.spec.runtime.policy.sandbox.image = image;
    return this;
  }

  agentLoop(command: string[]): this {
    this.spec.runtime.agent.mode = 'custom_image';
    this.spec.runtime.agent.known_agent_ref = undefined;
    const custom = this.ensureCustomImage();
    custom.entrypoint = [...command];
    return this;
  }

  agentArgs(args: string[]): this {
    const overrides = this.ensureAgentOverrides();
    overrides.args = [...args];
    return this;
  }

  agentEnv(env: Record<string, string>): this {
    const overrides = this.ensureAgentOverrides();
    overrides.env = { ...env };
    return this;
  }

  agentLoopEnv(env: Record<string, string>): this {
    return this.agentEnv(env);
  }

  agentEnvFromHost(keys: string[]): this {
    const overrides = this.ensureAgentOverrides();
    overrides.env_from_host = [...keys];
    return this;
  }

  agentLoopEnvFromHost(keys: string[]): this {
    return this.agentEnvFromHost(keys);
  }

  dependencyAssets(entries: DependencyAssetEntry[]): this {
    this.spec.runtime.dependencies.assets = entries.map((entry) => ({
      id: entry.id,
      source_from_host: entry.source_from_host,
      mount_path: entry.mount_path,
      read_only: entry.read_only ?? false,
      required: entry.required ?? true,
    }));
    this.spec.runtime.dependencies.file_staging = this.spec.runtime.dependencies.assets.map((entry) => ({
      source_from_host: entry.source_from_host,
      destination_path: entry.mount_path,
      required: entry.required ?? true,
    }));
    return this;
  }

  dependencyFileStaging(entries: DependencyFileStagingEntry[]): this {
    this.spec.runtime.dependencies.file_staging = entries.map((entry) => ({
      source_from_host: entry.source_from_host,
      destination_path: entry.destination_path,
      required: entry.required ?? true,
    }));
    this.spec.runtime.dependencies.assets = entries.map((entry) => ({
      source_from_host: entry.source_from_host,
      mount_path: entry.destination_path,
      read_only: false,
      required: entry.required ?? true,
    }));
    return this;
  }

  stageDependencyAsset(
    sourceFromHost: string,
    mountPath: string,
    options?: { id?: string; readOnly?: boolean; required?: boolean },
  ): this {
    if (!this.spec.runtime.dependencies.assets) {
      this.spec.runtime.dependencies.assets = [];
    }
    this.spec.runtime.dependencies.assets.push({
      id: options?.id,
      source_from_host: sourceFromHost,
      mount_path: mountPath,
      read_only: options?.readOnly ?? false,
      required: options?.required ?? true,
    });
    if (!this.spec.runtime.dependencies.file_staging) {
      this.spec.runtime.dependencies.file_staging = [];
    }
    this.spec.runtime.dependencies.file_staging.push({
      source_from_host: sourceFromHost,
      destination_path: mountPath,
      required: options?.required ?? true,
    });
    return this;
  }

  stageDependencyFile(
    sourceFromHost: string,
    destinationPath: string,
    options?: { required?: boolean },
  ): this {
    return this.stageDependencyAsset(sourceFromHost, destinationPath, {
      required: options?.required ?? true,
    });
  }

  baseline(variantId: string, bindings: Bindings): this {
    this.spec.baseline = { variant_id: variantId, bindings: { ...bindings } };
    return this;
  }

  addVariant(variantId: string, bindings: Bindings): this {
    this.spec.variant_plan.push({ variant_id: variantId, bindings: { ...bindings } });
    return this;
  }

  benchmark(config: BenchmarkConfig): this {
    this.spec.benchmark = {
      policy: config.policy
        ? {
            ...config.policy,
            required_evidence_classes: config.policy.required_evidence_classes
              ? [...config.policy.required_evidence_classes]
              : undefined,
          }
        : undefined,
      adapter: config.adapter
        ? {
            command: [...config.adapter.command],
            manifest: config.adapter.manifest
              ? JSON.parse(JSON.stringify(config.adapter.manifest)) as Record<string, unknown>
              : undefined,
          }
        : undefined,
    };
    return this;
  }

  replications(value: number): this {
    this.spec.design.replications = value;
    return this;
  }

  sanitizationProfile(value: string): this {
    this.spec.design.sanitization_profile = value;
    return this;
  }

  randomSeed(value: number): this {
    this.spec.design.random_seed = value;
    return this;
  }

  maxConcurrency(value: number): this {
    this.spec.design.max_concurrency = value;
    return this;
  }

  /** Set design policies directly (overrides any preset from .from()). */
  policies(value: DesignPolicies): this {
    this.spec.design.policies = copyPolicies(value);
    this.spec.design.comparison = value.comparison;
    return this;
  }

  /** Add a metric definition. Use Metric.* constants or Metric.fromOutput() / Metric.fromEvents(). */
  metric(def: MetricDef): this {
    // Replace existing metric with same id (allows overriding predefined defs)
    const idx = this.spec.metrics.findIndex((m) => m.id === def.id);
    if (idx >= 0) {
      this.spec.metrics[idx] = { ...def };
    } else {
      this.spec.metrics.push({ ...def });
    }
    return this;
  }

  /** Add a budget guardrail. Use Metric.max*() factories for common limits. */
  guardrail(def: GuardrailDef): this {
    if (!this.spec.guardrails) {
      this.spec.guardrails = [];
    }
    const idx = this.spec.guardrails.findIndex((g) => g.metric_id === def.metric_id);
    if (idx >= 0) {
      this.spec.guardrails[idx] = { ...def };
    } else {
      this.spec.guardrails.push({ ...def });
    }
    return this;
  }

  /** Configure artifact collection from the workspace after each trial. */
  artifacts(options: { collect: string[]; diff?: boolean; baseDir?: string }): this {
    this.spec.artifacts = {
      collect: [...options.collect],
      diff: options.diff ?? false,
      base_dir: options.baseDir,
    };
    return this;
  }

  networkMode(mode: 'none' | 'full' | 'allowlist_enforced', allowedHosts: string[] = []): this {
    this.spec.runtime.policy.network.mode = mode;
    this.spec.runtime.policy.network.allowed_hosts = [...allowedHosts];
    return this;
  }

  sandboxImage(image: string): this {
    this.spec.runtime.policy.sandbox.mode = 'container';
    this.spec.runtime.policy.sandbox.image = image;
    if (this.spec.runtime.agent.mode === 'custom_image') {
      const custom = this.ensureCustomImage();
      custom.image = image;
    }
    return this;
  }

  localSandbox(): this {
    this.spec.runtime.policy.sandbox = { mode: 'local' };
    return this;
  }

  timeoutMs(value: number): this {
    this.spec.runtime.policy.timeout_ms = value;
    return this;
  }

  build(): ExperimentSpec {
    const missing: string[] = [];
    if (!this.spec.experiment.id) missing.push('experiment id (call .id() or use ExperimentBuilder.create())');
    if (!this.spec.experiment.name) missing.push('experiment name (call .name() or use ExperimentBuilder.create())');
    if (!this.spec.dataset.path) missing.push('dataset path (call .datasetJsonl())');
    if (!this.spec.dataset.suite_id) missing.push('dataset suite_id (call .datasetJsonl() with suiteId)');
    if (!this.spec.dataset.split_id) missing.push('dataset split_id (call .datasetJsonl() with splitId)');
    if (this.spec.dataset.limit <= 0) missing.push('dataset limit (call .datasetJsonl() with limit > 0)');
    if (!this.spec.runtime.agent.mode) {
      missing.push('runtime agent mode (call .agentRef() or .agentLoop()/customAgentImage())');
    } else if (this.spec.runtime.agent.mode === 'known_agent_ref') {
      const known = this.spec.runtime.agent.known_agent_ref;
      if (!known?.id || known.id.trim().length === 0) {
        missing.push('runtime.agent.known_agent_ref.id (call .agentRef())');
      }
      if (!known?.version || known.version.trim().length === 0) {
        missing.push('runtime.agent.known_agent_ref.version (call .agentRef())');
      }
    } else if (this.spec.runtime.agent.mode === 'custom_image') {
      const entrypoint = this.spec.runtime.agent.custom_image?.entrypoint ?? [];
      if (entrypoint.length === 0 || entrypoint.some((part) => part.trim().length === 0)) {
        missing.push('runtime.agent.custom_image.entrypoint (call .agentLoop() or .customAgentImage())');
      }
    }
    const adapterId = this.spec.runtime.agent.adapter?.id?.trim() ?? '';
    const adapterVersion = this.spec.runtime.agent.adapter?.version?.trim() ?? '';
    if (adapterId.length === 0 && adapterVersion.length > 0) {
      missing.push('runtime.agent.adapter.id (call .agentAdapter() with a non-empty id)');
    }
    if (adapterVersion.length === 0 && adapterId.length > 0) {
      missing.push('runtime.agent.adapter.version (call .agentAdapter() with a non-empty version)');
    }
    if (this.spec.runtime.policy.timeout_ms <= 0) {
      missing.push('policy timeout_ms (call .timeoutMs() with > 0)');
    }
    if (this.spec.runtime.policy.sandbox.mode === 'container') {
      const customImage = this.spec.runtime.agent.custom_image?.image;
      const policyImage = this.spec.runtime.policy.sandbox.image;
      const hasImage = this.spec.runtime.agent.mode === 'known_agent_ref'
        || (!!customImage && customImage.trim().length > 0)
        || (!!policyImage && policyImage.trim().length > 0);
      if (!hasImage) {
        missing.push('container image (call .customAgentImage(), .sandboxImage(), or use .agentRef())');
      }
    }
    if (missing.length > 0) {
      throw new Error(
        `ExperimentBuilder: required fields not set:\n${missing.map((m) => `  - ${m}`).join('\n')}`,
      );
    }

    // Policy coherence validation
    const policies = this.spec.design.policies;
    if (policies) {
      const errors: string[] = [];
      const treatmentCount = this.spec.variant_plan.length;
      const totalVariants = treatmentCount + 1; // baseline + treatments

      if (policies.comparison === 'paired' && totalVariants < 2) {
        errors.push('paired comparison requires at least one treatment variant (call .addVariant())');
      }
      if (policies.scheduling === 'paired_interleaved' && totalVariants < 2) {
        errors.push('paired_interleaved scheduling requires at least 2 variants');
      }
      if (policies.retry.max_attempts < 1) {
        errors.push('retry.max_attempts must be >= 1');
      }
      if (errors.length > 0) {
        throw new Error(
          `ExperimentBuilder: policy coherence errors:\n${errors.map((e) => `  - ${e}`).join('\n')}`,
        );
      }
    }

    return JSON.parse(JSON.stringify(this.spec)) as ExperimentSpec;
  }

  toYaml(): string {
    return yamlStringify(this.build());
  }
}
