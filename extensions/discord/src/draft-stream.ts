import { createFinalizableDraftLifecycle } from "openclaw/plugin-sdk/channel-lifecycle";
import { formatErrorMessage } from "openclaw/plugin-sdk/error-runtime";
import {
  createChannelMessage,
  deleteChannelMessage,
  editChannelMessage,
  type RequestClient,
} from "./internal/discord.js";
import { resolveDiscordMessageFlags } from "./send.shared.js";

/** Discord messages cap at 2000 characters. */
const DISCORD_STREAM_MAX_CHARS = 2000;
const DEFAULT_THROTTLE_MS = 1200;
const DISCORD_PREVIEW_ALLOWED_MENTIONS = { parse: [] };

type DiscordDraftStream = {
  update: (text: string) => void;
  flush: () => Promise<void>;
  messageId: () => string | undefined;
  /** All slot message ids in order (oldest first). For multi-message streams. */
  messageIds: () => string[];
  clear: () => Promise<void>;
  /** Delete ALL slot messages (multi-message aware version of clear). */
  clearAll: () => Promise<void>;
  discardPending: () => Promise<void>;
  seal: () => Promise<void>;
  stop: () => Promise<void>;
  /**
   * Flush any pending text to the current message, then reset internal state
   * so the next update creates a new message. Callers MUST await this when
   * the preceding update() carried content that should land in the current
   * message before the new one begins — otherwise the throttle queue can
   * race the pending text with subsequent updates, producing duplicated or
   * out-of-order Discord messages.
   */
  forceNewMessage: () => Promise<void>;
};

export function createDiscordDraftStream(params: {
  rest: RequestClient;
  channelId: string;
  maxChars?: number;
  replyToMessageId?: string | (() => string | undefined);
  throttleMs?: number;
  /** Minimum chars before sending first message (debounce for push notifications) */
  minInitialChars?: number;
  suppressEmbeds?: boolean;
  log?: (message: string) => void;
  warn?: (message: string) => void;
}): DiscordDraftStream {
  const maxChars = Math.min(params.maxChars ?? DISCORD_STREAM_MAX_CHARS, DISCORD_STREAM_MAX_CHARS);
  const throttleMs = Math.max(250, params.throttleMs ?? DEFAULT_THROTTLE_MS);
  const minInitialChars = params.minInitialChars;
  const channelId = params.channelId;
  const rest = params.rest;
  const flags = resolveDiscordMessageFlags({ suppressEmbeds: params.suppressEmbeds });
  const resolveReplyToMessageId = () =>
    typeof params.replyToMessageId === "function"
      ? params.replyToMessageId()
      : params.replyToMessageId;

  const streamState = { stopped: false, final: false };
  // Idempotent multi-message state:
  //   messageIds[i] = Discord message id for chunk slot i (undefined until created)
  //   lastChunks[i] = last content sent to slot i (skip edit if unchanged)
  // Every update(text) recomputes chunks from scratch and reconciles each slot.
  // This makes the stream a pure function of the latest cumulative text — no
  // racing, no duplication, no out-of-order writes possible.
  const messageIds: Array<string | undefined> = [];
  const lastChunks: string[] = [];

  // Walk forward through the cumulative text, slicing into <=maxChars chunks.
  // Within each chunk, prefer to break at a logical boundary (paragraph >
  // line > sentence > word) when one exists in the trailing 25% of the
  // window; otherwise hard-cut at maxChars. After picking each boundary we
  // balance unclosed markdown markers (** __ * _ ``` `) by appending a
  // closer to the current chunk and reopening at the start of the next.
  const findSplitPoint = (text: string, start: number, hardMax: number): number => {
    const end = start + hardMax;
    if (end >= text.length) return text.length;
    const minAcceptable = start + Math.floor(hardMax * 0.75);
    const window = text.slice(start, end);
    const offset = (rel: number) => start + rel;
    const para = window.lastIndexOf("\n\n");
    if (offset(para) >= minAcceptable) return offset(para) + 2;
    const nl = window.lastIndexOf("\n");
    if (offset(nl) >= minAcceptable) return offset(nl) + 1;
    let sentence = -1;
    for (const marker of [". ", "! ", "? "]) {
      const idx = window.lastIndexOf(marker);
      if (idx > sentence) sentence = idx;
    }
    if (offset(sentence) >= minAcceptable) return offset(sentence) + 2;
    const space = window.lastIndexOf(" ");
    if (offset(space) >= minAcceptable) return offset(space) + 1;
    return end;
  };

  // Count markdown markers in a chunk and return which ones are unclosed,
  // ordered for proper close/reopen. Only the unambiguous markers are
  // balanced (`**`, `__`, ```` ``` ````, `` ` ``); single `*` and `_` are
  // skipped because file paths (`node_modules`, `use_state`) and code
  // contain them constantly without intending italic, and Discord's italic
  // rendering is word-boundary sensitive enough that lone counts are noisy.
  const detectOrphanMarkers = (chunk: string): string[] => {
    const orphans: string[] = [];
    let inFence = false;
    let inInline = false;
    let fenceCount = 0;
    let inlineCount = 0;
    let i = 0;
    const counts: Record<string, number> = { "**": 0, __: 0 };
    while (i < chunk.length) {
      if (!inInline && chunk.startsWith("```", i)) {
        fenceCount++;
        inFence = !inFence;
        i += 3;
        continue;
      }
      if (!inFence && chunk[i] === "`") {
        inlineCount++;
        inInline = !inInline;
        i += 1;
        continue;
      }
      if (inFence || inInline) {
        i += 1;
        continue;
      }
      if (chunk.startsWith("**", i)) {
        counts["**"]++;
        i += 2;
        continue;
      }
      if (chunk.startsWith("__", i)) {
        counts.__++;
        i += 2;
        continue;
      }
      i += 1;
    }
    if (fenceCount % 2 === 1) orphans.push("```");
    else if (inlineCount % 2 === 1) orphans.push("`");
    for (const marker of ["**", "__"] as const) {
      if (counts[marker] % 2 === 1) orphans.push(marker);
    }
    return orphans;
  };

  const computeChunks = (text: string): string[] => {
    const trimmed = text.trimEnd();
    if (!trimmed) return [];
    const out: string[] = [];
    let pos = 0;
    let prefix = "";
    while (pos < trimmed.length) {
      const remaining = trimmed.length - pos;
      const room = maxChars - prefix.length;
      if (remaining + prefix.length <= maxChars) {
        out.push(prefix + trimmed.slice(pos));
        break;
      }
      let splitAt = findSplitPoint(trimmed, pos, room);
      // Never bisect a 2-char marker (`**`, `__`, double-backtick). If splitAt
      // lands between two identical marker chars, back off by 1 so the whole
      // marker stays in the NEXT slot. Otherwise the orphan detector sees a
      // lone trailing `*` and injects a closer, leaving a stray `*` on its
      // own at the end of the previous Discord message.
      while (
        splitAt > pos + 1 &&
        splitAt < trimmed.length &&
        (trimmed[splitAt - 1] === "*" ||
          trimmed[splitAt - 1] === "_" ||
          trimmed[splitAt - 1] === "`") &&
        trimmed[splitAt - 1] === trimmed[splitAt]
      ) {
        splitAt -= 1;
      }
      let chunk = prefix + trimmed.slice(pos, splitAt);
      const orphans = detectOrphanMarkers(chunk);
      let nextPrefix = "";
      if (orphans.length > 0) {
        // Close orphans at end of this chunk; reopen at start of next so the
        // visual style continues across the message boundary.
        const closer = orphans.slice().reverse().join("");
        const opener = orphans.join("");
        if (chunk.length + closer.length <= maxChars) {
          chunk = chunk + closer;
          nextPrefix = opener;
        }
      }
      out.push(chunk);
      pos = splitAt;
      prefix = nextPrefix;
    }
    return out;
  };

  const sendOrEditStreamMessage = async (text: string): Promise<boolean> => {
    // Allow final flush even if stopped (e.g., after clear()).
    if (streamState.stopped && !streamState.final) {
      return false;
    }
    const chunks = computeChunks(text);
    if (chunks.length === 0) {
      return false;
    }

    // First-message debounce: hold off until the cumulative text reaches
    // minInitialChars (better push-notification quality). Only applies before
    // any message has been sent.
    if (
      messageIds.length === 0 &&
      minInitialChars != null &&
      !streamState.final &&
      text.length < minInitialChars
    ) {
      return false;
    }

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
      const existingId = messageIds[i];
      const existingChunk = lastChunks[i];

      if (existingId !== undefined && existingChunk === chunk) {
        // Slot unchanged — no API call needed.
        continue;
      }

      if (existingId === undefined) {
        // Create new message for this slot. message_reference (reply-to) only
        // attaches to the FIRST slot so we don't reply N times.
        const replyToMessageId = i === 0 ? resolveReplyToMessageId()?.trim() : undefined;
        const messageReference = replyToMessageId
          ? { message_id: replyToMessageId, fail_if_not_exists: false }
          : undefined;
        try {
          const sent = await createChannelMessage<{ id?: string }>(rest, channelId, {
            body: {
              content: chunk,
              allowed_mentions: DISCORD_PREVIEW_ALLOWED_MENTIONS,
              ...(flags ? { flags } : {}),
              ...(messageReference ? { message_reference: messageReference } : {}),
            },
          });
          const sentMessageId = sent?.id;
          if (typeof sentMessageId !== "string" || !sentMessageId) {
            streamState.stopped = true;
            params.warn?.(
              `discord stream preview stopped (missing message id from send, slot=${i})`,
            );
            return false;
          }
          messageIds[i] = sentMessageId;
          lastChunks[i] = chunk;
        } catch (err) {
          streamState.stopped = true;
          params.warn?.(
            `discord stream preview create failed (slot=${i}): ${formatErrorMessage(err)}`,
          );
          return false;
        }
      } else {
        // Edit existing message in this slot with the new chunk content.
        try {
          await editChannelMessage(rest, channelId, existingId, {
            body: {
              content: chunk,
              allowed_mentions: DISCORD_PREVIEW_ALLOWED_MENTIONS,
              ...(flags ? { flags } : {}),
            },
          });
          lastChunks[i] = chunk;
        } catch (err) {
          streamState.stopped = true;
          params.warn?.(
            `discord stream preview edit failed (slot=${i}): ${formatErrorMessage(err)}`,
          );
          return false;
        }
      }
    }
    return true;
  };

  // The lifecycle's clear() path deletes a single "current" message id and
  // then nulls it. With multi-slot we treat the LAST slot as "the current
  // message" for that purpose — anything before it is already user-visible
  // content that shouldn't be deleted by the lifecycle's clear/discard hooks.
  const readMessageId = (): string | undefined => {
    for (let i = messageIds.length - 1; i >= 0; i--) {
      const id = messageIds[i];
      if (typeof id === "string") return id;
    }
    return undefined;
  };
  const clearMessageId = () => {
    // Pop the last slot only; preserve earlier frozen messages.
    if (messageIds.length === 0) return;
    messageIds.pop();
    lastChunks.pop();
  };
  const isValidStreamMessageId = (value: unknown): value is string => typeof value === "string";
  const deleteStreamMessage = async (messageId: string) => {
    await deleteChannelMessage(rest, channelId, messageId);
  };

  const { loop, update, stop, clear, discardPending, seal } = createFinalizableDraftLifecycle({
    throttleMs,
    state: streamState,
    sendOrEditStreamMessage,
    readMessageId,
    clearMessageId,
    isValidMessageId: isValidStreamMessageId,
    deleteMessage: deleteStreamMessage,
    warn: params.warn,
    warnPrefix: "discord stream preview cleanup failed",
  });

  // Reset multi-slot state so the next update() starts a fresh sequence of
  // messages. Awaits a flush so any pending text lands in the current slots
  // before we detach — eliminates the throttle race that was producing
  // duplicated content in Discord.
  const forceNewMessage = async (): Promise<void> => {
    await loop.flush();
    messageIds.length = 0;
    lastChunks.length = 0;
    loop.resetPending();
  };

  // Multi-slot aware cleanup. The lifecycle's clear() only deletes ONE
  // message id, which was correct for the original single-message design
  // but stranded earlier slots when we moved to multi-message. clearAll()
  // stops the stream, deletes every slot's Discord message, and resets state.
  const clearAll = async (): Promise<void> => {
    console.log(`[discord-stream] clearAll: ENTRY, messageIds.length=${messageIds.length}`);
    streamState.stopped = true;
    loop.stop();
    await loop.waitForInFlight();
    const ids = messageIds.filter((id): id is string => typeof id === "string");
    console.log(`[discord-stream] clearAll: deleting ${ids.length} slot(s) [${ids.join(", ")}]`);
    messageIds.length = 0;
    lastChunks.length = 0;
    loop.resetPending();
    for (const id of ids) {
      try {
        await deleteChannelMessage(rest, channelId, id);
        console.log(`[discord-stream] clearAll: deleted ${id}`);
      } catch (err) {
        console.log(`[discord-stream] clearAll: delete ${id} FAILED: ${formatErrorMessage(err)}`);
      }
    }
  };

  const readAllMessageIds = (): string[] =>
    messageIds.filter((id): id is string => typeof id === "string");

  params.log?.(`discord stream preview ready (maxChars=${maxChars}, throttleMs=${throttleMs})`);

  return {
    update,
    flush: loop.flush,
    messageId: readMessageId,
    messageIds: readAllMessageIds,
    clear,
    clearAll,
    discardPending,
    seal,
    stop,
    forceNewMessage,
  };
}
