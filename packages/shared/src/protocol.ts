/**
 * gateway ↔ loop-node（worker）之间 WebSocket 通信的消息契约，两端共享此文件。
 * 消息按方向分为两个以 type 为判别字段的联合：
 *   - WorkerToGateway：注册 / 心跳 / 运行进度与结果；
 *   - GatewayToWorker：注册应答、launch 下发、cancel 与 ping。
 * 扩展时在对应联合追加成员，type 沿用命名空间前缀（如 run.*、hello.*），收发两端需同步实现。
 * 下方 EventSource / HarnessEvent 属于事件日志体系，与上述 WS 消息相互独立：
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

/**
 * 节点级重试与熔断策略。
 *
 * 定义在 shared 而非 gateway：策略由业务模板（templates/*.md）声明，
 * 模板由执行面加载——而架构边界要求 Gateway 不读模板内容、不理解业务语义。
 * 因此执行面解析模板后，必须随 run.result 把策略回报给控制面，
 * 控制面才有依据做熔断判定。类型放 shared 是为了两侧口径一致。
 */
export interface RetryPolicy {
  /** 最大失败尝试次数；第 N 次失败即触发熔断 */
  maxAttempts: number;
  /** 累计注入 token 上限，超过即熔断 */
  maxTokens: number;
  /** 单节点最长执行时间（ms），超过即熔断 */
  maxDurationMs: number;
  /** 熔断后的处置：转人工卡点 / 直接失败 / 跳过该节点 */
  onExhausted: 'hitl' | 'fail' | 'skip';
}

/**
 * 默认策略：仅当模板未声明 retry 段时兜底。
 * 模板声明优先——业务方按节点风险自行决定重试预算（如 QA 节点容忍更多次）。
 * 放在 shared 是因为执行面解析模板时也要用它兜底，而执行面不能反向依赖控制面。
 */
export const DEFAULT_RETRY: RetryPolicy = {
  maxAttempts: 3,
  maxTokens: 5_000_000,
  maxDurationMs: 3_600_000,
  onExhausted: 'hitl',
};

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
      /**
       * 本次执行的节点模板所声明的重试策略。
       * 由执行面解析模板 frontmatter 后回报，控制面据此替代全局默认值做熔断判定。
       */
      retry?: RetryPolicy;
      /** 前置检查结果摘要，便于控制面区分「环境问题」与「业务失败」。 */
      preflight?: { passed: boolean; failed: string[] };
      /**
       * worker 侧耗时：从收到 launch 到发出本次结果（含模板加载/preflight/prompt 落盘/agent 执行）。
       * 控制面另有按 dispatch 时刻计算的端到端耗时，两者口径不同，各自上报。
       */
      durationMs?: number;
      /**
       * agent 真实用量：从 run 工作目录 trajectories/*.json 的 llm_interactions[].response.usage
       * 原样累加（input_tokens / output_tokens）。缺省表示拿不到 transcript（如 mock agent、
       * agent 启动前失败），控制面沿用注入 token 估算口径。
       */
      tokens?: { input: number; output: number };
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
