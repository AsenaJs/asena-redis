import { describe, expect, it } from 'bun:test';
import * as pkg from '../index';

describe('package export surface', () => {
  it('should export the stream helpers as functions', () => {
    const names = [
      'xadd',
      'xgroupCreate',
      'xgroupDelConsumer',
      'xreadgroup',
      'xrange',
      'xack',
      'xpending',
      'xpendingConsumer',
      'xinfoConsumers',
      'xclaim',
      'entryTimestamp',
      'normalizeStreamsReply',
      'normalizeEntries',
      'normalizeFields',
    ];

    for (const name of names) {
      expect(typeof (pkg as Record<string, unknown>)[name]).toBe('function');
    }
  });
});
