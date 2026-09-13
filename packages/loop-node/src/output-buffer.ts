/**
 * 输出有界缓冲。
 *
 * 为什么必须有界：agent 会话输出可能达数百 MB（长推理 + 大量工具调用日志）。
 * 无上限累积会撑爆 worker 内存，且终态 run.result 要把它整个塞进一个 WS 帧。
 * 整条链路都以「尾部即结论」为前提 —— Gateway 侧入库截 800 字符、
 * 写回 issue 评论只取尾部 3000 字符，保留全文没有意义。
 *
 * 代价：丢失会话中段输出。需要完整 transcript 时依赖 agent 自身的本地会话文件。
 *
 * 策略是「超过上限才裁剪」而非「滚动窗口逐块裁剪」：
 * 前者只在越界那一刻做一次 slice，长会话下是 O(1) 次操作；
 * 后者每块都要 slice + 拼接，高频输出时是明显热点。
 */
export interface OutputBuffer {
  /** 追加一块输出 */
  append(chunk: string): void;
  /** 当前缓冲内容（至多 maxBytes，越界后为尾部 keepBytes） */
  readonly value: string;
  /** 是否发生过截断（用于在 error 信息里提示「输出已按尾部截断」） */
  readonly truncated: boolean;
}

export interface OutputBufferOptions {
  /** 触发裁剪的阈值（字符数），超过后裁剪到 keep */
  max: number;
  /** 裁剪后保留的尾部长度（字符数） */
  keep: number;
}

export function createOutputBuffer(opts: OutputBufferOptions): OutputBuffer {
  const { max, keep } = opts;
  let buf = '';
  let truncated = false;

  return {
    append(chunk) {
      if (!chunk) return;
      buf += chunk;
      // keep >= max 时裁剪没有意义（裁完反而更长），只在配置合理时生效
      if (buf.length > max && keep < max) {
        buf = buf.slice(-keep);
        truncated = true;
      }
    },
    get value() {
      return buf;
    },
    get truncated() {
      return truncated;
    },
  };
}
