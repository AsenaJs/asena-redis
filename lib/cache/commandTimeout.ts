/** Far above a healthy command on a local Redis (p99 under 0.2 ms) and below a stall a user notices. */
export const DEFAULT_CACHE_TIMEOUT_MS = 500;

/** The largest delay `setTimeout` honours; above it the timer fires after about 1 ms. */
export const MAX_CACHE_TIMEOUT_MS = 2_147_483_647;

/**
 * Settles like `command`, or rejects with `<name> timed out after <ms>ms`.
 *
 * The command is not cancelled: a client with an offline queue may still send it later.
 */
export const withTimeout = async <T>(command: Promise<T>, ms: number, name: string): Promise<T> => {
  // The command that loses the race may still reject; that must not become an unhandled rejection.
  command.catch(() => {});

  let timer: ReturnType<typeof setTimeout> | undefined;

  try {
    return await Promise.race([
      command,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${name} timed out after ${ms}ms`)), ms);
        timer.unref?.();
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
};
