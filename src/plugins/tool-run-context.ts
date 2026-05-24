/**
 * Per-call run context for plugin-owned tools.
 *
 * The agent-tool `execute(toolCallId, params, signal, onUpdate)` contract
 * has no slot for runtime session/run identity. Hooks get a populated `ctx`
 * via the lifecycle helpers, but tool execute calls receive nothing — which
 * forced the temporal-memory plugin to inject identity into the system
 * prompt and have the model echo it back as params (hacky, and the model
 * can omit / mistype them).
 *
 * This module exposes a host-owned AsyncLocalStorage that the tool-execute
 * adapter wraps around every plugin tool call. Plugins read the active
 * identity via `getCurrentToolRunContext()` without any cooperation from
 * the model or special tool params.
 */

import { AsyncLocalStorage } from "node:async_hooks";

export type PluginToolRunContext = {
  agentId?: string;
  sessionId?: string;
  sessionKey?: string;
  runId?: string;
  channelId?: string;
  workspaceDir?: string;
};

const pluginToolRunContextStorage = new AsyncLocalStorage<PluginToolRunContext>();

/**
 * Run `fn` with the supplied run context bound as the active plugin tool ctx.
 * Nested calls override; missing fields fall through to the inner-most call.
 */
export function runWithPluginToolRunContext<T>(
  ctx: PluginToolRunContext | undefined,
  fn: () => T,
): T {
  if (!ctx) {
    return fn();
  }
  return pluginToolRunContextStorage.run(ctx, fn);
}

/**
 * Read the active plugin tool run context, if any. Returns undefined when
 * called outside a wrapped tool execute (e.g. from a hook handler — hooks
 * get their own ctx parameter and shouldn't rely on this).
 */
export function getCurrentPluginToolRunContext(): PluginToolRunContext | undefined {
  return pluginToolRunContextStorage.getStore();
}
