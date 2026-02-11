import { stringify as yamlStringify } from 'yaml';

export type Bindings = Record<string, unknown>;

export interface DatasetJsonlOptions {
  suiteId: string;
  provider?: 'local_jsonl';
  schemaVersion?: string;
  splitId: string;
  limit: number;
}

export interface HarnessCliOptions {
  integrationLevel: 'cli_basic' | 'cli_events' | 'otel' | 'sdk_control' | 'sdk_full';
  inputPath?: string;
  outputPath?: string;
}

export interface ExperimentSpec {
  version: '0.3';
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
    comparison: 'paired' | 'unpaired';
    replications: number;
    random_seed: number;
    shuffle_tasks: boolean;
    max_concurrency: number;
  };
  analysis_plan: {
    primary_metrics: string[];
    secondary_metrics: string[];
    missingness: {
      policy: string;
      record_reasons: boolean;
    };
    tests: Record<string, unknown>;
    multiple_comparisons: {
      method: string;
    };
    reporting: {
      effect_sizes: string[];
      show_task_level_table: boolean;
    };
  };
  baseline: {
    variant_id: string;
    bindings: Bindings;
  };
  variant_plan: Array<{
    variant_id: string;
    bindings: Bindings;
  }>;
  runtime: {
    harness: {
      mode: 'cli';
      command: string[];
      integration_level: string;
      input_path: string;
      output_path: string;
      control_plane: {
        mode: 'file';
        path: string;
      };
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
    network: {
      mode: 'none' | 'full' | 'allowlist_enforced';
      allowed_hosts: string[];
    };
  };
  validity: {
    fail_on_state_leak: boolean;
    fail_on_profile_invariant_violation: boolean;
  };
}

function defaultTests(primaryMetrics: string[], secondaryMetrics: string[]): Record<string, unknown> {
  const names = Array.from(new Set([...primaryMetrics, ...secondaryMetrics]));
  const tests: Record<string, unknown> = {};
  for (const name of names) {
    tests[name] = { method: 'paired_bootstrap', ci: 0.95, resamples: 1000 };
  }
  return tests;
}

export class ExperimentBuilder {
  private readonly spec: ExperimentSpec;

  static create(id: string, name: string): ExperimentBuilder {
    return new ExperimentBuilder(id, name);
  }

  private constructor(id: string, name: string) {
    this.spec = {
      version: '0.3',
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
        sanitization_profile: '',
        comparison: 'paired',
        replications: 0,
        random_seed: 0,
        shuffle_tasks: true,
        max_concurrency: 1,
      },
      analysis_plan: {
        primary_metrics: ['success'],
        secondary_metrics: ['latency_ms'],
        missingness: {
          policy: 'paired_drop',
          record_reasons: true,
        },
        tests: defaultTests(['success'], ['latency_ms']),
        multiple_comparisons: {
          method: 'none',
        },
        reporting: {
          effect_sizes: ['risk_diff', 'median_diff'],
          show_task_level_table: true,
        },
      },
      baseline: {
        variant_id: 'base',
        bindings: {},
      },
      variant_plan: [],
      runtime: {
        harness: {
          mode: 'cli',
          command: [],
          integration_level: '',
          input_path: '/out/trial_input.json',
          output_path: '/out/trial_output.json',
          control_plane: {
            mode: 'file',
            path: '/state/lab_control.json',
          },
        },
        sandbox: { mode: 'local' },
        network: {
          mode: 'none',
          allowed_hosts: [],
        },
      },
      validity: {
        fail_on_state_leak: true,
        fail_on_profile_invariant_violation: true,
      },
    };
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

  harnessCli(command: string[], options: HarnessCliOptions): this {
    this.spec.runtime.harness.command = [...command];
    this.spec.runtime.harness.integration_level = options.integrationLevel;
    this.spec.runtime.harness.input_path = options.inputPath ?? this.spec.runtime.harness.input_path;
    this.spec.runtime.harness.output_path = options.outputPath ?? this.spec.runtime.harness.output_path;
    return this;
  }

  baseline(variantId: string, bindings: Bindings): this {
    this.spec.baseline = { variant_id: variantId, bindings: { ...bindings } };
    return this;
  }

  addVariant(variantId: string, bindings: Bindings): this {
    this.spec.variant_plan.push({ variant_id: variantId, bindings: { ...bindings } });
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

  primaryMetrics(values: string[]): this {
    this.spec.analysis_plan.primary_metrics = [...values];
    this.spec.analysis_plan.tests = defaultTests(values, this.spec.analysis_plan.secondary_metrics);
    return this;
  }

  secondaryMetrics(values: string[]): this {
    this.spec.analysis_plan.secondary_metrics = [...values];
    this.spec.analysis_plan.tests = defaultTests(this.spec.analysis_plan.primary_metrics, values);
    return this;
  }

  networkMode(mode: 'none' | 'full' | 'allowlist_enforced', allowedHosts: string[] = []): this {
    this.spec.runtime.network.mode = mode;
    this.spec.runtime.network.allowed_hosts = [...allowedHosts];
    return this;
  }

  sandboxImage(image: string): this {
    this.spec.runtime.sandbox.mode = 'container';
    this.spec.runtime.sandbox.image = image;
    return this;
  }

  localSandbox(): this {
    this.spec.runtime.sandbox = { mode: 'local' };
    return this;
  }

  build(): ExperimentSpec {
    const missing: string[] = [];
    if (!this.spec.dataset.path) missing.push('dataset path (call .datasetJsonl())');
    if (!this.spec.dataset.suite_id) missing.push('dataset suite_id (call .datasetJsonl() with suiteId)');
    if (!this.spec.dataset.split_id) missing.push('dataset split_id (call .datasetJsonl() with splitId)');
    if (this.spec.dataset.limit <= 0) missing.push('dataset limit (call .datasetJsonl() with limit > 0)');
    if (!this.spec.design.sanitization_profile) missing.push('sanitization_profile (call .sanitizationProfile())');
    if (this.spec.design.replications <= 0) missing.push('replications (call .replications() with value > 0)');
    if (this.spec.design.random_seed === 0) missing.push('random_seed (call .randomSeed())');
    if (this.spec.runtime.harness.command.length === 0) missing.push('harness command (call .harnessCli())');
    if (!this.spec.runtime.harness.integration_level) missing.push('harness integration_level (call .harnessCli() with integrationLevel)');
    if (missing.length > 0) {
      throw new Error(
        `ExperimentBuilder: required fields not set:\n${missing.map((m) => `  - ${m}`).join('\n')}`,
      );
    }
    return JSON.parse(JSON.stringify(this.spec)) as ExperimentSpec;
  }

  toYaml(): string {
    return yamlStringify(this.build());
  }
}
