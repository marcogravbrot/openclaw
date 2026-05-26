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

  // Hard-cut the cumulative text into maxChars-sized chunks. Marco's directive:
  // don't care WHERE the split lands, just get every char through in order.
  // Skipping fancy boundary logic eliminates the markdown-split / mid-word /
  // race-with-caller issues we kept tripping over.
  const computeChunks = (text: string): string[] => {
    const trimmed = text.trimEnd();
    if (!trimmed) return [];
    const out: string[] = [];
    for (let i = 0; i < trimmed.length; i += maxChars) {
      out.push(trimmed.slice(i, i + maxChars));
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
    streamState.stopped = true;
    loop.stop();
    await loop.waitForInFlight();
    const ids = messageIds.filter((id): id is string => typeof id === "string");
    messageIds.length = 0;
    lastChunks.length = 0;
    loop.resetPending();
    for (const id of ids) {
      try {
        await deleteChannelMessage(rest, channelId, id);
      } catch (err) {
        params.warn?.(
          `discord stream preview clearAll: delete ${id} failed: ${formatErrorMessage(err)}`,
        );
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
