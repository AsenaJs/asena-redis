import 'reflect-metadata';
import type { RedisDecoratorOptions } from '../types';
import { AsenaRedisService } from '../AsenaRedisService';
import { Service } from '@asenajs/asena/decorators';
import { defineMetadata, getMetadata, getMetadataKeys } from 'reflect-metadata/no-conflict';

export function Redis(options: RedisDecoratorOptions) {
  return function <T extends new (...args: any[]) => any>(target: T) {
    @Service(options.name || target.name)
    class RedisServiceClass extends AsenaRedisService {

      public constructor() {
        super();

        if (!options.logger) {
          options.logger = console;
        }

        this.setRedisOptions(options);

        if (options.client) {
          this.setRedisClient(options.client);
        }
      }

    }

    // Copy prototype methods from target
    Object.getOwnPropertyNames(target.prototype).forEach((name) => {
      if (name !== 'constructor') {
        const descriptor = Object.getOwnPropertyDescriptor(target.prototype, name);

        if (descriptor) {
          Object.defineProperty(RedisServiceClass.prototype, name, descriptor);
        }
      }
    });

    // Copy static methods and properties
    Object.getOwnPropertyNames(target).forEach((name) => {
      if (name !== 'prototype' && name !== 'name' && name !== 'length') {
        const descriptor = Object.getOwnPropertyDescriptor(target, name);

        if (descriptor) {
          Object.defineProperty(RedisServiceClass, name, descriptor);
        }
      }
    });

    // Copy metadata
    const metadata = getMetadataKeys(target);

    metadata.forEach((key) => {
      const value = getMetadata(key, target);

      defineMetadata(key, value, RedisServiceClass);
    });

    // Override class name to match original target
    Object.defineProperty(RedisServiceClass, 'name', {
      value: target.name,
      writable: false,
      configurable: true,
    });

    return RedisServiceClass as any;
  };
}