export { LabClient, LabRunnerError } from './client.js';
export { ExperimentBuilder } from './experiment-builder.js';

export type {
  DescribeArgs,
  DescribeResponse,
  ExperimentSummary,
  HooksValidateArgs,
  ImageBuildArgs,
  ImageBuildResponse,
  KnobsValidateArgs,
  LabClientOptions,
  LabErrorEnvelope,
  LabErrorPayload,
  PublishArgs,
  PublishResponse,
  RunArgs,
  RunDevArgs,
  RunExperimentArgs,
  RunResponse,
  SchemaValidateArgs,
  ValidateResponse,
} from './types.js';

export type { ExperimentSpec, DatasetJsonlOptions, HarnessCliOptions } from './experiment-builder.js';
