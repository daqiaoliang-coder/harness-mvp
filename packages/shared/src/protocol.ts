/**
 * 四类事件源：
 *  - github   外部工作流平台事件（对应 Meego）
 *  - openapi  API 调用事件
 *  - harness  编排事件（Gateway 内部决策）
 *  - worker   执行事件（loop-node / Agent）
 */
export type EventSource = 'github' | 'openapi' | 'harness' | 'worker';

export interface HarnessEvent {
  id: string;
  seq: number;
  timestamp: string;
  source: EventSource;
  event: string;
  projectId: string;
  runId?: string;
  nodeId?: string;
  workItemId?: string;
  details?: Record<string, unknown>;
}

/** Worker → Gateway */
export type WorkerToGateway =
  | { type: 'hello'; nodeId: string; token: string; agents: string[]; version: string }
  | { type: 'heartbeat'; nodeId: string; load: number }
  | { type: 'run.progress'; runId: string; chunk: string }
  | {
      type: 'run.result';
      runId: string;
      status: 'completed' | 'failed';
      output?: string;
      error?: string;
    };

/** Gateway → Worker */
export type GatewayToWorker =
  | { type: 'hello.ack'; nodeId: string; serverTime: string }
  | { type: 'hello.reject'; reason: string }
  | {
      type: 'launch';
      runId: string;
      workItemId: string;
      nodeKey: string;
      template: string;
      context: Record<string, string>;
    }
  | { type: 'cancel'; runId: string }
  | { type: 'ping' };
