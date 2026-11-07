import { describe, expect, it, spyOn } from 'bun:test';
import { join } from 'node:path';
import { DEFAULT_CACHE_TIMEOUT_MS, withTimeout } from '../lib/cache/commandTimeout';

describe('withTimeout', () => {
  it('settles with the command when it answers in time', async () => {
    expect(await withTimeout(Promise.resolve('OK'), 1000, 'SET')).toBe('OK');
    await expect(withTimeout(Promise.reject(new Error('down')), 1000, 'SET')).rejects.toThrow('down');
  });

  it('rejects with the command name and the limit once the limit passes', async () => {
    await expect(withTimeout(new Promise(() => {}), 20, 'GET')).rejects.toThrow('GET timed out after 20ms');
  });

  it('clears its timer when the command settles first', async () => {
    const cleared = spyOn(globalThis, 'clearTimeout');

    try {
      await withTimeout(Promise.resolve(1), 60_000, 'GET');

      expect(cleared).toHaveBeenCalledTimes(1);
      expect(cleared.mock.calls[0]![0]).toBeDefined();
    } finally {
      cleared.mockRestore();
    }
  });

  it('does not surface a late rejection of a command that lost the race', async () => {
    let rejectLate!: (error: Error) => void;
    const late = new Promise<never>((_, reject) => {
      rejectLate = reject;
    });
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);

    process.on('unhandledRejection', onUnhandled);

    try {
      await expect(withTimeout(late, 10, 'GET')).rejects.toThrow('timed out');
      rejectLate(new Error('connection closed'));
      await Bun.sleep(10);

      expect(unhandled).toEqual([]);
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });

  it('never keeps the process alive', async () => {
    // A pending cache command must not stop an application from exiting.
    const module = join(import.meta.dir, '../lib/cache/commandTimeout.ts');
    const child = Bun.spawn(
      [
        'bun',
        '-e',
        `import { withTimeout } from ${JSON.stringify(module)}; withTimeout(new Promise(() => {}), 60000, 'GET');`,
      ],
      { stdout: 'ignore', stderr: 'pipe' },
    );
    const killer = setTimeout(() => child.kill(), 10_000);
    const started = performance.now();

    try {
      expect(await child.exited).toBe(0);
      expect(performance.now() - started).toBeLessThan(5000);
    } finally {
      clearTimeout(killer);
    }
  }, 15_000);

  it('defaults to 500 ms', () => {
    expect(DEFAULT_CACHE_TIMEOUT_MS).toBe(500);
  });
});
