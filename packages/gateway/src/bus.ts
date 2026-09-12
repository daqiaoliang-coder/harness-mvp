import { EventEmitter } from 'node:events';
import type { HarnessEvent } from '@harness/shared';

export class EventBus {
  private emitter = new EventEmitter();

  constructor() {
    this.emitter.setMaxListeners(0);
  }

  emitEvent(ev: HarnessEvent) {
    this.emitter.emit('event', ev);
  }

  onEvent(fn: (ev: HarnessEvent) => void): () => void {
    this.emitter.on('event', fn);
    return () => this.emitter.off('event', fn);
  }
}
