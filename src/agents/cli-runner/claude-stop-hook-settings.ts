/**
 * Builds and persists a per-session Claude Code settings file that wires a
 * Stop hook back into the OpenClaw native-hook relay, so plugin
 * `before_agent_finalize` handlers can revise / force-stop a turn.
 *
 * Why per-session (not per-run): the claude-cli live session is a long-lived
 * process whose settings are loaded once at spawn (via `--settings <path>`),
 * but a single live session handles many runs. We pin the relayId to the
 * sessionId, write the settings file once per session, and let each run
 * re-register the relay under the same id at run start. Re-registration
 * cleanly replaces the prior record (see `registerNativeHookRelay`).
 */

import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { buildNativeHookRelayCommand } from "../harness/native-hook-relay.js";

export type ClaudeStopHookSettings = {
  /** Absolute path to the generated settings JSON, suitable for `claude --settings`. */
  settingsPath: string;
  /** Stable relay id the registered hook will look up at invocation time. */
  relayId: string;
};

export function prepareClaudeStopHookSettings(params: {
  sessionId: string;
}): ClaudeStopHookSettings {
  const relayId = stableRelayIdForSession(params.sessionId);
  const command = buildNativeHookRelayCommand({
    provider: "claude-cli",
    relayId,
    event: "before_agent_finalize",
  });
  const settings = {
    hooks: {
      Stop: [
        {
          hooks: [{ type: "command", command }],
        },
      ],
    },
  };
  const dir = path.join(tmpdir(), "openclaw-claude-hooks");
  mkdirSync(dir, { recursive: true });
  const settingsPath = path.join(dir, `stop-${relayId}.json`);
  writeFileSync(settingsPath, `${JSON.stringify(settings, null, 2)}\n`, "utf8");
  return { settingsPath, relayId };
}

/**
 * Derives a URL-safe, compact id from the sessionId. The native-hook relay
 * imposes its own id format constraints; hashing keeps us inside them even
 * for sessionIds that contain colons or other separators.
 */
function stableRelayIdForSession(sessionId: string): string {
  const digest = createHash("sha256").update(`claude-cli:${sessionId}`).digest("hex");
  return `claude-cli-${digest.slice(0, 32)}`;
}
