/**
 * 进程内事件总线：Scheduler / WS 层产生的 HarnessEvent 经此 fan-out 给 SSE 等订阅方。
 * 事件必须先经 EventStore 落库拿到全局 seq，再到这里广播（见 scheduler.emit / ws 的 emit），
 * 保证订阅方拿到的每一条事件都可被断线补发，不会出现「广播过但库里没有」。
 */
import { EventEmitter } from 'node:events';
import type { HarnessEvent } from '@harness/shared';

export class EventBus {
  private emitter = new EventEmitter();

  constructor() {
    // 监听器数量随 SSE 连接数增长，关闭 10 个默认上限，避免在线看板多时触发 MaxListeners 告警
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
