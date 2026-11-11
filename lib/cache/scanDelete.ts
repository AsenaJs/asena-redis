/** Sends one raw command; the caller decides how long each one may take. */
export type SendCommand = (command: string, args: string[]) => Promise<unknown>;

const SCAN_COUNT = 500;

// A reply that never reaches cursor '0' would otherwise spin forever inside a request.
const MAX_ITERATIONS = 10_000;

interface ScanReply {
  cursor: string;
  keys: string[];
}

// `send` is Promise<any>: an unexpected shape must stop the loop, not be coerced into key names.
const parseScanReply = (reply: unknown): ScanReply | null => {
  if (!Array.isArray(reply) || reply.length < 2) return null;

  const [cursor, keys] = reply as [unknown, unknown];

  if (!Array.isArray(keys)) return null;

  return { cursor: String(cursor), keys: keys.map((key) => String(key)) };
};

/**
 * Deletes every key matching `pattern`, in batches, without blocking the server.
 *
 * SCAN is not a snapshot, so a key written during the walk may survive until its TTL. It is also
 * per-node, so under Redis Cluster this would clear only the node it reaches.
 *
 * @returns How many keys were handed to DEL.
 */
export const deleteByPattern = async (
  send: SendCommand,
  pattern: string,
  onUnexpectedReply: (message: string) => void,
): Promise<number> => {
  let cursor = '0';
  let deleted = 0;
  let iterations = 0;

  do {
    if (++iterations > MAX_ITERATIONS) {
      onUnexpectedReply(`SCAN over '${pattern}' did not finish within ${MAX_ITERATIONS} rounds; giving up`);
      break;
    }

    const reply = parseScanReply(await send('SCAN', [cursor, 'MATCH', pattern, 'COUNT', String(SCAN_COUNT)]));

    if (!reply) {
      onUnexpectedReply(`SCAN over '${pattern}' returned an unrecognized reply; giving up`);
      break;
    }

    cursor = reply.cursor;

    // `DEL` with no arguments is a syntax error, and an empty SCAN round is normal.
    if (reply.keys.length > 0) {
      await send('DEL', reply.keys);
      deleted += reply.keys.length;
    }
  } while (cursor !== '0');

  return deleted;
};
