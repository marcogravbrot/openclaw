export type EmbeddedRunStageTiming = {
  name: string;
  durationMs: number;
  elapsedMs: number;
};

export type EmbeddedRunStageSummary = {
  totalMs: number;
  stages: EmbeddedRunStageTiming[];
};

export type EmbeddedRunStageTracker = {
  mark: (name: string) => void;
  snapshot: () => EmbeddedRunStageSummary;
};

export const EMBEDDED_RUN_ATTEMPT_DISPATCH_STAGE = {
  workspace: "attempt-workspace",
  prompt: "attempt-prompt",
  runtimePlan: "attempt-runtime-plan",
  dispatch: "attempt-dispatch",
} as const;

const EMBEDDED_RUN_STAGE_WARN_TOTAL_MS = 10_000;
const EMBEDDED_RUN_STAGE_WARN_STAGE_MS = 5_000;

export function createEmbeddedRunStageTracker(options?: {
  now?: () => number;
  /**
   * Optional tag (typically runId) for synchronous stderr breadcrumbs.
   * When set, each `mark()` writes one line directly to process.stderr,
   * which is synchronous in Node when stderr is a terminal or pipe. This
   * bypasses the event loop so stage transitions remain visible even when
   * sync CPU work has starved the loop (which kills async timers and
   * log-subsystem flushes).
   */
  syncBreadcrumbTag?: string;
}): EmbeddedRunStageTracker {
  const now = options?.now ?? Date.now;
  const startedAt = now();
  let previousAt = startedAt;
  const stages: EmbeddedRunStageTiming[] = [];
  const breadcrumbTag = options?.syncBreadcrumbTag;

  const toMs = (value: number) => Math.max(0, Math.round(value));

  return {
    mark(name) {
      const currentAt = now();
      const durationMs = toMs(currentAt - previousAt);
      const elapsedMs = toMs(currentAt - startedAt);
      stages.push({ name, durationMs, elapsedMs });
      previousAt = currentAt;
      if (breadcrumbTag) {
        try {
          process.stderr.write(
            `[prep-breadcrumb] ${new Date(currentAt).toISOString()} run=${breadcrumbTag} stage=${name} stageDurMs=${durationMs} totalElapsedMs=${elapsedMs}\n`,
          );
        } catch {
          // never let diagnostics crash the run
        }
      }
    },
    snapshot() {
      return {
        totalMs: toMs(now() - startedAt),
        stages: stages.slice(),
      };
    },
  };
}

export function shouldWarnEmbeddedRunStageSummary(
  summary: EmbeddedRunStageSummary,
  options?: {
    totalThresholdMs?: number;
    stageThresholdMs?: number;
  },
): boolean {
  const totalThresholdMs = options?.totalThresholdMs ?? EMBEDDED_RUN_STAGE_WARN_TOTAL_MS;
  const stageThresholdMs = options?.stageThresholdMs ?? EMBEDDED_RUN_STAGE_WARN_STAGE_MS;
  return (
    summary.totalMs >= totalThresholdMs ||
    summary.stages.some((stage) => stage.durationMs >= stageThresholdMs)
  );
}

export function formatEmbeddedRunStageSummary(
  prefix: string,
  summary: EmbeddedRunStageSummary,
): string {
  const stages =
    summary.stages.length > 0
      ? summary.stages
          .map((stage) => `${stage.name}:${stage.durationMs}ms@${stage.elapsedMs}ms`)
          .join(",")
      : "none";
  return `${prefix} totalMs=${summary.totalMs} stages=${stages}`;
}
