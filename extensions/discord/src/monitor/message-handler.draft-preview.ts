import { EmbeddedBlockChunker } from "openclaw/plugin-sdk/agent-runtime";
import {
  createChannelProgressDraftGate,
  type ChannelProgressDraftLine,
  formatChannelProgressDraftText,
  isChannelProgressDraftWorkToolName,
  mergeChannelProgressDraftLine,
  normalizeChannelProgressDraftLineIdentity,
  resolveChannelProgressDraftMaxLines,
  resolveChannelStreamingBlockEnabled,
  resolveChannelStreamingPreviewToolProgress,
  resolveChannelStreamingSuppressDefaultToolProgressMessages,
} from "openclaw/plugin-sdk/channel-streaming";
import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";
import {
  convertMarkdownTables,
  stripInlineDirectiveTagsForDelivery,
  stripReasoningTagsFromText,
} from "openclaw/plugin-sdk/text-chunking";
import { chunkDiscordTextWithMode } from "../chunk.js";
import { resolveDiscordDraftStreamingChunking } from "../draft-chunking.js";
import { createDiscordDraftStream } from "../draft-stream.js";
import type { RequestClient } from "../internal/discord.js";
import { resolveDiscordPreviewStreamMode } from "../preview-streaming.js";

type DraftReplyReference = {
  peek: () => string | undefined;
};

type DiscordConfig = NonNullable<OpenClawConfig["channels"]>["discord"];

export function createDiscordDraftPreviewController(params: {
  cfg: OpenClawConfig;
  discordConfig: DiscordConfig;
  accountId: string;
  sourceRepliesAreToolOnly: boolean;
  textLimit: number;
  deliveryRest: RequestClient;
  deliverChannelId: string;
  replyReference: DraftReplyReference;
  tableMode: Parameters<typeof convertMarkdownTables>[1];
  maxLinesPerMessage: number | undefined;
  chunkMode: Parameters<typeof chunkDiscordTextWithMode>[1]["chunkMode"];
  log: (message: string) => void;
}) {
  const discordStreamMode = resolveDiscordPreviewStreamMode(params.discordConfig);
  const draftMaxChars = Math.min(params.textLimit, 2000);
  const accountBlockStreamingEnabled =
    resolveChannelStreamingBlockEnabled(params.discordConfig) ??
    params.cfg.agents?.defaults?.blockStreamingDefault === "on";
  const canStreamProgressDraftForToolOnlySource =
    params.sourceRepliesAreToolOnly && discordStreamMode === "progress";
  const canStreamDraft =
    (!params.sourceRepliesAreToolOnly || canStreamProgressDraftForToolOnlySource) &&
    discordStreamMode !== "off" &&
    !accountBlockStreamingEnabled;
  const draftStream = canStreamDraft
    ? createDiscordDraftStream({
        rest: params.deliveryRest,
        channelId: params.deliverChannelId,
        maxChars: draftMaxChars,
        replyToMessageId: () => params.replyReference.peek(),
        minInitialChars: discordStreamMode === "progress" ? 0 : 30,
        suppressEmbeds: params.discordConfig?.suppressEmbeds ?? true,
        throttleMs: 1200,
        log: params.log,
        warn: params.log,
      })
    : undefined;
  const draftChunking =
    draftStream && discordStreamMode === "block"
      ? resolveDiscordDraftStreamingChunking(params.cfg, params.accountId)
      : undefined;
  const shouldSplitPreviewMessages = discordStreamMode === "block";
  const draftChunker = draftChunking ? new EmbeddedBlockChunker(draftChunking) : undefined;
  let lastPartialText = "";
  let draftText = "";
  let hasStreamedMessage = false;
  let finalizedViaPreviewMessage = false;
  let finalReplyStarted = false;
  let finalReplyDelivered = false;

  // ── camus/discord-streaming-fix ─────────────────────────────────────────────
  // Chronological timeline of segments for "partial" mode, so each text block
  // and each tool call summary appends to the same Discord message instead of
  // overwriting prior content. See extensions/discord/CAMUS_PATCH.md.
  type CamusTextSegment = { kind: "text"; text: string };
  type CamusToolSegment = {
    kind: "tool";
    key: string;
    line: string | ChannelProgressDraftLine;
  };
  type CamusSegment = CamusTextSegment | CamusToolSegment;
  const camusTimeline: CamusSegment[] = [];
  let camusActiveText = "";
  // Cumulative cleaned text last seen from the runtime; used for shrink detection
  // (claude-cli sometimes sends a shorter cumulative snapshot mid-turn — ignore those).
  let camusRawActive = "";
  // Number of characters in the cumulative cleaned text already committed to the
  // timeline (as text segments) via assistant-message-boundary / tool-progress
  // events. The next partial peels active text off as `cleaned.slice(committedLen)`.
  // This gives us real per-block separation driven by runtime events instead of
  // text-content heuristics.
  let camusCommittedLen = 0;
  // OPENCLAW_DISCORD_HIDE_TOOL_PROGRESS=1|true|yes hides the tool-call lines
  // (🔧 Bash, 🔧 Read, etc.) in the partial-mode timeline. The boundary commit
  // around each tool still fires, so paragraph separation is unaffected.
  const camusShowToolProgress = !/^(?:1|true|yes|on)$/i.test(
    process.env.OPENCLAW_DISCORD_HIDE_TOOL_PROGRESS ?? "",
  );

  const camusToolKey = (line: string | ChannelProgressDraftLine): string => {
    if (typeof line === "string") return `string:${line}`;
    const name = line.toolName ?? "tool";
    const kind = line.kind ?? "";
    const detail = typeof line.detail === "string" ? line.detail.slice(0, 40) : "";
    return `${name}|${kind}|${detail}`;
  };

  const camusCommitActiveText = () => {
    // Advance the committed-length cursor regardless — this is what makes future
    // partials peel new text off correctly when an event-driven boundary fires.
    camusCommittedLen = camusRawActive.length;
    if (camusActiveText.trim().length === 0) {
      camusActiveText = "";
      return;
    }
    camusTimeline.push({ kind: "text", text: camusActiveText });
    camusActiveText = "";
  };

  const camusFormatToolLine = (line: string | ChannelProgressDraftLine): string => {
    if (typeof line === "string") return `> ${line}`;
    // `line.text` from the plugin-sdk builder is the fully-rendered display
    // string and already begins with the tool's emoji (e.g. "📖 Read: foo.ts",
    // "🛠️ echo hello", "🛠️ stage git changes"). Just wrap it in a Discord
    // blockquote — don't add another emoji or backticks.
    if (typeof line.text === "string" && line.text.trim()) {
      const status = line.status ? ` [${line.status}]` : "";
      return `> ${line.text.trim()}${status}`;
    }
    const name = line.toolName ?? "tool";
    const status = line.status ? ` [${line.status}]` : "";
    const detail = line.detail ? `: ${line.detail}` : "";
    return `> 🔧 \`${name}\`${status}${detail}`;
  };

  const camusRenderSegments = (includeActive: boolean): string => {
    // Render segments in order. Adjacent tool segments are joined with a
    // single newline so a burst of back-to-back tool calls renders tight;
    // text↔tool and text↔text transitions use a blank line (\n\n).
    const renderedSegs = camusTimeline.map((seg) =>
      seg.kind === "text"
        ? { kind: "text" as const, text: seg.text }
        : { kind: "tool" as const, text: camusFormatToolLine(seg.line) },
    );
    if (includeActive && camusActiveText) {
      renderedSegs.push({ kind: "text", text: camusActiveText });
    }
    let out = "";
    for (let i = 0; i < renderedSegs.length; i += 1) {
      const seg = renderedSegs[i];
      if (i > 0) {
        const prev = renderedSegs[i - 1];
        out += prev.kind === "tool" && seg.kind === "tool" ? "\n" : "\n\n";
      }
      out += seg.text;
    }
    return out;
  };

  const camusRender = (): string => camusRenderSegments(true);

  // Pick a natural split point in `text` at or before `maxLen` chars. Prefers
  // paragraph break, then line break, then sentence end, then word boundary.
  // Falls back to a hard cut at `maxLen` only when no break is reasonably near.
  const camusFindSplitPoint = (text: string, maxLen: number): number => {
    if (text.length <= maxLen) return text.length;
    const window = text.slice(0, maxLen);
    const minAcceptable = Math.floor(maxLen * 0.5);
    const paragraph = window.lastIndexOf("\n\n");
    if (paragraph >= minAcceptable) return paragraph + 2;
    const newline = window.lastIndexOf("\n");
    if (newline >= minAcceptable) return newline + 1;
    const sentence = Math.max(
      window.lastIndexOf(". "),
      window.lastIndexOf("! "),
      window.lastIndexOf("? "),
    );
    if (sentence >= minAcceptable) return sentence + 2;
    const space = window.lastIndexOf(" ");
    if (space >= minAcceptable) return space + 1;
    return maxLen;
  };

  const camusUpdateStream = async (): Promise<void> => {
    if (!draftStream) return;
    const body = camusRender();
    if (!body) return;
    // No caller-side rollover: draftStream is fully idempotent and handles
    // multi-message splitting internally. Each update() call re-renders the
    // FULL cumulative body across N Discord message slots — slots that didn't
    // change skip their API call. This eliminates the throttle-queue race
    // between firstBody/forceNewMessage/body that produced duplicated
    // headings and out-of-order messages.
    hasStreamedMessage = true;
    lastPartialText = body;
    draftStream.update(body);
  };

  const camusResetTimeline = () => {
    camusTimeline.length = 0;
    camusActiveText = "";
    camusRawActive = "";
    camusCommittedLen = 0;
  };
  // ── /camus ──────────────────────────────────────────────────────────────────
  const previewToolProgressEnabled =
    Boolean(draftStream) && resolveChannelStreamingPreviewToolProgress(params.discordConfig);
  const suppressDefaultToolProgressMessages =
    Boolean(draftStream) &&
    resolveChannelStreamingSuppressDefaultToolProgressMessages(params.discordConfig, {
      draftStreamActive: true,
      previewToolProgressEnabled,
    });
  let previewToolProgressSuppressed = false;
  let previewToolProgressLines: Array<string | ChannelProgressDraftLine> = [];
  let reasoningProgressRawText = "";
  let lastReasoningProgressLine: string | undefined;
  const progressSeed = `${params.accountId}:${params.deliverChannelId}`;

  const renderProgressDraft = async (options?: { flush?: boolean }) => {
    if (!draftStream || discordStreamMode !== "progress") {
      return;
    }
    const previewText = formatChannelProgressDraftText({
      entry: params.discordConfig,
      lines: previewToolProgressLines,
      seed: progressSeed,
    });
    if (!previewText || previewText === lastPartialText) {
      return;
    }
    lastPartialText = previewText;
    draftText = previewText;
    hasStreamedMessage = true;
    draftChunker?.reset();
    draftStream.update(previewText);
    if (options?.flush) {
      await draftStream.flush();
    }
  };

  const progressDraftGate = createChannelProgressDraftGate({
    onStart: () => renderProgressDraft({ flush: true }),
  });

  const resetProgressState = () => {
    lastPartialText = "";
    draftText = "";
    draftChunker?.reset();
    previewToolProgressSuppressed = false;
    previewToolProgressLines = [];
    reasoningProgressRawText = "";
    lastReasoningProgressLine = undefined;
  };

  const forceNewMessageIfNeeded = async (): Promise<void> => {
    if (shouldSplitPreviewMessages && hasStreamedMessage) {
      params.log("discord: calling forceNewMessage() for draft stream");
      await draftStream?.forceNewMessage();
    }
    resetProgressState();
  };

  return {
    draftStream,
    previewToolProgressEnabled,
    suppressDefaultToolProgressMessages,
    get isProgressMode() {
      return discordStreamMode === "progress";
    },
    get hasProgressDraftStarted() {
      return progressDraftGate.hasStarted;
    },
    get finalizedViaPreviewMessage() {
      return finalizedViaPreviewMessage;
    },
    markFinalReplyStarted() {
      finalReplyStarted = true;
    },
    markFinalReplyDelivered() {
      finalReplyDelivered = true;
    },
    markPreviewFinalized() {
      finalizedViaPreviewMessage = true;
    },
    disableBlockStreamingForDraft: draftStream ? true : undefined,
    async startProgressDraft() {
      if (!draftStream || discordStreamMode !== "progress") {
        return;
      }
      await progressDraftGate.startNow();
    },
    async pushToolProgress(
      line?: string | ChannelProgressDraftLine,
      options?: { toolName?: string; toolCallId?: string },
    ) {
      if (!draftStream) {
        return;
      }
      if (finalReplyStarted || finalReplyDelivered) {
        return;
      }
      if (
        options?.toolName !== undefined &&
        !isChannelProgressDraftWorkToolName(options.toolName)
      ) {
        return;
      }
      if (isEmptyDiscordProgressLine(line)) {
        return;
      }
      const normalized = normalizeChannelProgressDraftLineIdentity(line);
      if (!normalized) {
        return;
      }
      const progressLine: string | ChannelProgressDraftLine =
        typeof line === "object" && line !== undefined ? line : normalized;
      if (discordStreamMode === "partial") {
        // camus: every tool start is also a natural assistant-block boundary,
        // so commit the active text into the timeline regardless of whether
        // we render the tool line itself. This is what gives the user
        // properly separated text segments around tool calls.
        camusCommitActiveText();
        if (!camusShowToolProgress) {
          await camusUpdateStream();
          return;
        }
        // Dedup by per-invocation toolCallId when provided (so progress updates
        // for the SAME call replace the prior render) and fall back to the
        // rendered text otherwise. Using the text alone collapses distinct
        // calls that the plugin-sdk happens to format identically (e.g. two
        // `echo` commands both rendered as "🛠️ print text").
        const key = options?.toolCallId ? `id:${options.toolCallId}` : camusToolKey(progressLine);
        const last = camusTimeline[camusTimeline.length - 1];
        if (last && last.kind === "tool" && last.key === key) {
          last.line = progressLine;
        } else {
          camusTimeline.push({ kind: "tool", key, line: progressLine });
        }
        await camusUpdateStream();
        return;
      }
      if (discordStreamMode !== "progress") {
        if (!previewToolProgressEnabled || previewToolProgressSuppressed) {
          return;
        }
        const nextLines = mergeChannelProgressDraftLine(previewToolProgressLines, progressLine, {
          maxLines: resolveChannelProgressDraftMaxLines(params.discordConfig),
        });
        if (nextLines === previewToolProgressLines) {
          return;
        }
        previewToolProgressLines = nextLines;
        const previewText = formatChannelProgressDraftText({
          entry: params.discordConfig,
          lines: previewToolProgressLines,
          seed: progressSeed,
        });
        lastPartialText = previewText;
        draftText = previewText;
        hasStreamedMessage = true;
        draftChunker?.reset();
        draftStream.update(previewText);
        return;
      }
      if (previewToolProgressEnabled && !previewToolProgressSuppressed && normalized) {
        previewToolProgressLines = mergeChannelProgressDraftLine(
          previewToolProgressLines,
          progressLine,
          {
            maxLines: resolveChannelProgressDraftMaxLines(params.discordConfig),
          },
        );
      }
      const alreadyStarted = progressDraftGate.hasStarted;
      if (shouldStartDiscordProgressDraftNow(line)) {
        await progressDraftGate.startNow();
      } else {
        await progressDraftGate.noteWork();
      }
      if (alreadyStarted && progressDraftGate.hasStarted) {
        await renderProgressDraft();
      }
    },
    async pushReasoningProgress(text?: string) {
      if (!draftStream || discordStreamMode !== "progress" || !text) {
        return;
      }
      if (finalReplyDelivered) {
        return;
      }
      reasoningProgressRawText = mergeReasoningProgressText(reasoningProgressRawText, text);
      const normalized = normalizeReasoningProgressLine(reasoningProgressRawText);
      if (!normalized) {
        return;
      }
      if (previewToolProgressEnabled && !previewToolProgressSuppressed) {
        const priorIndex =
          lastReasoningProgressLine === undefined
            ? -1
            : previewToolProgressLines.lastIndexOf(lastReasoningProgressLine);
        if (priorIndex >= 0) {
          previewToolProgressLines = [...previewToolProgressLines];
          previewToolProgressLines[priorIndex] = normalized;
        } else {
          previewToolProgressLines = [...previewToolProgressLines, normalized].slice(
            -resolveChannelProgressDraftMaxLines(params.discordConfig),
          );
        }
        lastReasoningProgressLine = normalized;
      }
      const alreadyStarted = progressDraftGate.hasStarted;
      await progressDraftGate.noteWork();
      if (alreadyStarted && progressDraftGate.hasStarted) {
        await renderProgressDraft();
      }
    },
    async resolvePreviewFinalText(text?: string) {
      if (discordStreamMode === "partial") {
        // camus: the streamed timeline IS the final message. Commit any
        // trailing active text, then return the rendered body so the caller
        // edits the existing draft instead of falling through to deliverNormally
        // (which would post a fresh reply containing only the last block of
        // assistant text from claude-cli, throwing the streamed body away).
        camusCommitActiveText();
        await camusUpdateStream();
        return camusRender() || undefined;
      }
      if (typeof text !== "string") {
        return undefined;
      }
      const formatted = convertMarkdownTables(
        stripInlineDirectiveTagsForDelivery(text).text,
        params.tableMode,
      );
      const chunks = chunkDiscordTextWithMode(formatted, {
        maxChars: draftMaxChars,
        maxLines: params.maxLinesPerMessage,
        chunkMode: params.chunkMode,
      });
      if (!chunks.length && formatted) {
        chunks.push(formatted);
      }
      if (chunks.length !== 1) {
        return undefined;
      }
      const trimmed = chunks[0].trim();
      if (!trimmed) {
        return undefined;
      }
      const currentPreviewText = discordStreamMode === "block" ? draftText : lastPartialText;
      if (
        currentPreviewText &&
        currentPreviewText.startsWith(trimmed) &&
        trimmed.length < currentPreviewText.length
      ) {
        return undefined;
      }
      return trimmed;
    },
    async updateFromPartial(text?: string) {
      if (!draftStream || !text) {
        return;
      }
      const rawCleaned = stripInlineDirectiveTagsForDelivery(
        stripReasoningTagsFromText(text, { mode: "strict", trim: "both" }),
      ).text;
      // Repair mid-word newlines that occasionally leak from claude-cli SSE
      // chunk boundaries (e.g. "log\nged"). A newline mid-word is never
      // legitimate prose — list/header/blockquote markers always trail a
      // newline with whitespace or a punctuation marker, so [a-z0-9]\n[a-z]
      // can only be a token boundary.
      const cleaned = rawCleaned.replace(/([A-Za-z0-9])\n([a-z])/g, "$1$2");
      if (!cleaned || cleaned.startsWith("Reasoning:\n")) {
        return;
      }
      if (cleaned === lastPartialText) {
        return;
      }
      if (discordStreamMode === "progress") {
        return;
      }
      previewToolProgressSuppressed = true;
      previewToolProgressLines = [];
      hasStreamedMessage = true;
      if (discordStreamMode === "partial") {
        // camus: track cumulative runtime text in `camusRawActive`. Peel
        // active block text off as `cleaned.slice(committedLen)` so prior
        // blocks already committed to the timeline (via boundary/tool events)
        // are not duplicated in the active region. Trim leading whitespace
        // from the peel — claude-cli's cumulative snapshots often include
        // `\n\n` between message blocks, which would compound with the
        // join-separator in camusRender to produce two blank lines.
        if (
          camusRawActive &&
          camusRawActive.startsWith(cleaned) &&
          cleaned.length < camusRawActive.length
        ) {
          return;
        }
        // Detect a cumulative restart (e.g. a stop-hook re-prompt mid-turn,
        // where the runtime begins a fresh assistant message with different
        // content). If the new cleaned text neither extends the prior raw
        // cumulative nor is a prefix of it, commit the prior active text to
        // the timeline so the new text appends below as its own block — not
        // replacing what was already shown.
        const cumulativeRestarted =
          camusRawActive &&
          !cleaned.startsWith(camusRawActive) &&
          !camusRawActive.startsWith(cleaned);
        if (cumulativeRestarted) {
          camusCommitActiveText();
          camusCommittedLen = 0;
          camusRawActive = "";
        } else if (cleaned.length < camusCommittedLen) {
          // Runtime sent a shorter snapshot than what we already committed; this
          // means the cumulative text restarted (new turn / resume). Reset.
          camusCommittedLen = 0;
        }
        camusRawActive = cleaned;
        camusActiveText = cleaned.slice(camusCommittedLen).replace(/^\s+/, "");
        await camusUpdateStream();
        return;
      }

      let delta = cleaned;
      if (cleaned.startsWith(lastPartialText)) {
        delta = cleaned.slice(lastPartialText.length);
      } else {
        draftChunker?.reset();
        draftText = "";
      }
      lastPartialText = cleaned;
      if (!delta) {
        return;
      }
      if (!draftChunker) {
        draftText = cleaned;
        draftStream.update(draftText);
        return;
      }
      draftChunker.append(delta);
      draftChunker.drain({
        force: false,
        emit: (chunk) => {
          draftText += chunk;
          draftStream.update(draftText);
        },
      });
    },
    async handleAssistantMessageBoundary() {
      if (discordStreamMode === "progress") {
        return;
      }
      if (discordStreamMode === "partial") {
        // camus: commit the in-progress assistant block to the timeline so the
        // next text block is appended below rather than replacing it.
        camusCommitActiveText();
        await camusUpdateStream();
        return;
      }
      await forceNewMessageIfNeeded();
    },
    async flush() {
      if (!draftStream) {
        return;
      }
      if (draftChunker?.hasBuffered()) {
        draftChunker.drain({
          force: true,
          emit: (chunk) => {
            draftText += chunk;
          },
        });
        draftChunker.reset();
        if (draftText) {
          draftStream.update(draftText);
        }
      }
      await draftStream.flush();
    },
    async cleanup() {
      try {
        progressDraftGate.cancel();
        if (!finalReplyDelivered) {
          await draftStream?.discardPending();
        }
        // camus: in partial mode, if we have already streamed visible content
        // to a Discord draft and no final reply was delivered (e.g. the agent
        // returned an empty response after running a background task), SEAL
        // the message instead of CLEARING it. Clearing deletes the streamed
        // body the user has already seen, which is destructive and surprising.
        // Sealing keeps the streamed content in place as the final message.
        const hasStreamedPartialBody =
          discordStreamMode === "partial" &&
          (camusTimeline.length > 0 || camusActiveText.trim().length > 0);
        if (!finalReplyDelivered && !finalizedViaPreviewMessage && draftStream?.messageId()) {
          if (hasStreamedPartialBody) {
            // Flush any in-flight active text into the timeline first so the
            // sealed message contains everything the runtime delivered.
            camusCommitActiveText();
            await camusUpdateStream();
            await draftStream.flush();
            await draftStream.seal();
          } else {
            // clearAll so all preview slots get deleted, not just the last.
            await draftStream.clearAll();
          }
        }
      } catch (err) {
        params.log(`discord: draft cleanup failed: ${String(err)}`);
      }
      camusResetTimeline();
    },
  };
}

function normalizeReasoningProgressLine(text: string): string {
  return text
    .replace(/^\s*(?:>\s*)?(?:Reasoning:|Thinking\.{0,3})\s*/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

function mergeReasoningProgressText(current: string, incoming: string): string {
  if (!current) {
    return incoming;
  }
  const normalizedCurrent = normalizeReasoningProgressLine(current);
  const normalizedIncoming = normalizeReasoningProgressLine(incoming);
  if (!normalizedIncoming || normalizedIncoming === normalizedCurrent) {
    return current;
  }
  if (isReasoningSnapshotText(incoming) || normalizedIncoming.startsWith(normalizedCurrent)) {
    return incoming;
  }
  return `${current}${incoming}`;
}

function isReasoningSnapshotText(text: string): boolean {
  return /^\s*(?:>\s*)?(?:Reasoning:|Thinking\.{0,3})\s*/i.test(text);
}

function isEmptyDiscordProgressLine(line: string | ChannelProgressDraftLine | undefined): boolean {
  if (!line || typeof line === "string") {
    return false;
  }
  return line.toolName === "apply_patch" && !line.detail && !line.status;
}

function shouldStartDiscordProgressDraftNow(
  line: string | ChannelProgressDraftLine | undefined,
): boolean {
  return typeof line === "object" && line?.kind === "patch" && Boolean(line.detail);
}
