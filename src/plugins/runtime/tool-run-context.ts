import { AsyncLocalStorage } from "node:async_hooks";
import { resolveGlobalSingleton } from "../../shared/global-singleton.js";

export type PluginToolRunContext = {
  runId?: string;
  sessionId?: string;
  sessionKey?: string;
  /** Provider name ("discord", "slack", …). Channel id is encoded in sessionKey. */
  messageProvider?: string;
  agentId?: string;
};

const PLUGIN_TOOL_RUN_CONTEXT_KEY: unique symbol = Symbol.for("openclaw.pluginToolRunContext");

const pluginToolRunContextStore = resolveGlobalSingleton<AsyncLocalStorage<PluginToolRunContext>>(
  PLUGIN_TOOL_RUN_CONTEXT_KEY,
  () => new AsyncLocalStorage<PluginToolRunContext>(),
);

export function withPluginToolRunContext<T>(ctx: PluginToolRunContext, run: () => T): T {
  return pluginToolRunContextStore.run(ctx, run);
}

export function getCurrentPluginToolRunContext(): PluginToolRunContext | undefined {
  return pluginToolRunContextStore.getStore();
}
