import { runCliAgent } from "../../agents/cli-runner.js";
import type { RunCliAgentParams } from "../../agents/cli-runner/types.js";
import type { EmbeddedPiRunResult } from "../../agents/pi-embedded.js";
import { emitAgentEvent, onAgentEvent } from "../../infra/agent-events.js";
import {
  normalizeLowercaseStringOrEmpty,
  normalizeOptionalString,
} from "../../shared/string-coerce.js";

function shouldBridgeCliAssistantTextToReasoning(provider: string): boolean {
  return normalizeLowercaseStringOrEmpty(provider) === "claude-cli";
}

function createAssistantTextBridge(params: {
  runId: string;
  suppressed?: boolean;
  deliver?: (text: string) => Promise<void>;
}) {
  const deliver = params.deliver;
  if (!deliver) {
    return {
      unsubscribe: () => undefined,
      drain: async (): Promise<void> => undefined,
    };
  }
  let lastText: string | undefined;
  let unsubscribed = false;
  let delivery = Promise.resolve();
  const rawUnsubscribe = onAgentEvent((evt) => {
    if (evt.runId !== params.runId || evt.stream !== "assistant") {
      return;
    }
    if (params.suppressed) {
      return;
    }
    const text = typeof evt.data.text === "string" ? evt.data.text : undefined;
    if (text === undefined || text === lastText) {
      return;
    }
    lastText = text;
    delivery = delivery.then(() => deliver(text)).catch(() => undefined);
  });
  return {
    unsubscribe() {
      if (unsubscribed) {
        return;
      }
      unsubscribed = true;
      rawUnsubscribe();
    },
    async drain(): Promise<void> {
      await delivery;
    },
  };
}

function createAssistantMessageStartBridge(params: {
  runId: string;
  suppressed?: boolean;
  notify?: () => Promise<void> | void;
}) {
  const notify = params.notify;
  if (!notify) {
    return {
      unsubscribe: () => undefined,
      drain: async (): Promise<void> => undefined,
    };
  }
  let unsubscribed = false;
  let delivery = Promise.resolve();
  const rawUnsubscribe = onAgentEvent((evt) => {
    if (evt.runId !== params.runId || evt.stream !== "assistant") return;
    if (params.suppressed) return;
    if (evt.data.phase !== "message-start") return;
    delivery = delivery.then(() => Promise.resolve(notify())).catch(() => undefined);
  });
  return {
    unsubscribe() {
      if (unsubscribed) return;
      unsubscribed = true;
      rawUnsubscribe();
    },
    async drain(): Promise<void> {
      await delivery;
    },
  };
}

function createAgentEventBridge(params: {
  runId: string;
  suppressed?: boolean;
  onAgentEvent?: (evt: {
    stream: string;
    data: Record<string, unknown>;
  }) => Promise<void> | void;
  streams: ReadonlyArray<string>;
}) {
  const handler = params.onAgentEvent;
  if (!handler) {
    return {
      unsubscribe: () => undefined,
      drain: async (): Promise<void> => undefined,
    };
  }
  const accepted = new Set(params.streams);
  let unsubscribed = false;
  let delivery = Promise.resolve();
  const rawUnsubscribe = onAgentEvent((evt) => {
    if (evt.runId !== params.runId) return;
    if (params.suppressed) return;
    if (!accepted.has(evt.stream)) return;
    const payload = { stream: evt.stream, data: evt.data };
    delivery = delivery.then(() => Promise.resolve(handler(payload))).catch(() => undefined);
  });
  return {
    unsubscribe() {
      if (unsubscribed) return;
      unsubscribed = true;
      rawUnsubscribe();
    },
    async drain(): Promise<void> {
      await delivery;
    },
  };
}

export async function runCliAgentWithLifecycle(params: {
  runId: string;
  provider: string;
  runParams: RunCliAgentParams;
  startedAt?: number;
  emitLifecycleStart?: boolean;
  emitLifecycleTerminal?: boolean;
  onAgentRunStart?: () => void;
  suppressAssistantBridge?: boolean;
  onAssistantText?: (text: string) => Promise<void>;
  onReasoningText?: (text: string) => Promise<void>;
  onAssistantMessageStart?: () => Promise<void> | void;
  onAgentEvent?: (evt: {
    stream: string;
    data: Record<string, unknown>;
  }) => Promise<void> | void;
  onErrorBeforeLifecycle?: (err: unknown) => Promise<void>;
  transformResult?: (result: EmbeddedPiRunResult) => EmbeddedPiRunResult;
}): Promise<EmbeddedPiRunResult> {
  const startedAt = params.startedAt ?? Date.now();
  const emitLifecycleStart = params.emitLifecycleStart ?? true;
  const emitLifecycleTerminal = params.emitLifecycleTerminal ?? true;
  params.onAgentRunStart?.();
  if (emitLifecycleStart) {
    emitAgentEvent({
      runId: params.runId,
      stream: "lifecycle",
      data: {
        phase: "start",
        startedAt,
      },
    });
  }
  const assistantBridge = createAssistantTextBridge({
    runId: params.runId,
    suppressed: params.suppressAssistantBridge,
    deliver: params.onAssistantText,
  });
  const reasoningBridge = createAssistantTextBridge({
    runId: params.runId,
    suppressed: params.suppressAssistantBridge,
    deliver: shouldBridgeCliAssistantTextToReasoning(params.provider)
      ? params.onReasoningText
      : undefined,
  });
  const messageStartBridge = createAssistantMessageStartBridge({
    runId: params.runId,
    suppressed: params.suppressAssistantBridge,
    notify: params.onAssistantMessageStart,
  });
  const toolEventBridge = createAgentEventBridge({
    runId: params.runId,
    suppressed: params.suppressAssistantBridge,
    onAgentEvent: params.onAgentEvent,
    streams: ["tool", "item", "plan", "command_output", "patch"],
  });
  const unsubscribeAllBridges = () => {
    assistantBridge.unsubscribe();
    reasoningBridge.unsubscribe();
    messageStartBridge.unsubscribe();
    toolEventBridge.unsubscribe();
  };
  const drainAllBridges = async () => {
    await assistantBridge.drain();
    await reasoningBridge.drain();
    await messageStartBridge.drain();
    await toolEventBridge.drain();
  };
  let lifecycleTerminalEmitted = false;
  try {
    const rawResult = await runCliAgent(params.runParams);
    const result = params.transformResult?.(rawResult) ?? rawResult;
    unsubscribeAllBridges();
    await drainAllBridges();

    const cliText = normalizeOptionalString(result.payloads?.[0]?.text);
    if (cliText) {
      emitAgentEvent({
        runId: params.runId,
        stream: "assistant",
        data: { text: cliText },
      });
    }

    if (emitLifecycleTerminal) {
      emitAgentEvent({
        runId: params.runId,
        stream: "lifecycle",
        data: {
          phase: "end",
          startedAt,
          endedAt: Date.now(),
        },
      });
      lifecycleTerminalEmitted = true;
    }
    return result;
  } catch (err) {
    unsubscribeAllBridges();
    await drainAllBridges();
    await params.onErrorBeforeLifecycle?.(err);
    if (emitLifecycleTerminal) {
      emitAgentEvent({
        runId: params.runId,
        stream: "lifecycle",
        data: {
          phase: "error",
          startedAt,
          endedAt: Date.now(),
          error: String(err),
        },
      });
      lifecycleTerminalEmitted = true;
    }
    throw err;
  } finally {
    unsubscribeAllBridges();
    if (emitLifecycleTerminal && !lifecycleTerminalEmitted) {
      emitAgentEvent({
        runId: params.runId,
        stream: "lifecycle",
        data: {
          phase: "error",
          startedAt,
          endedAt: Date.now(),
          error: "CLI run completed without lifecycle terminal event",
        },
      });
    }
  }
}
