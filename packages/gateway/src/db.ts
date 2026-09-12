import Database from 'better-sqlite3';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import type { HarnessEvent } from '@harness/shared';

interface RawRow {
  id: string;
  seq: number;
  timestamp: string;
  source: string;
  event: string;
  project_id: string;
  run_id: string | null;
  node_id: string | null;
  work_item_id: string | null;
  details: string | null;
}

function mapRow(r: RawRow): HarnessEvent {
  return {
    id: r.id,
    seq: r.seq,
    timestamp: r.timestamp,
    source: r.source as HarnessEvent['source'],
    event: r.event,
    projectId: r.project_id,
    runId: r.run_id ?? undefined,
    nodeId: r.node_id ?? undefined,
    workItemId: r.work_item_id ?? undefined,
    details: r.details ? JSON.parse(r.details) : {},
  };
}

/**
 * SQLite 事件存储。seq 是全局单调递增序号，
 * SSE 断线补发时用 Last-Event-ID = seq 即可继续。
 */
export class EventStore {
  private db: Database.Database;
  private seq = 0;

  constructor(file: string) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    this.db = new Database(file);
    this.db.pragma('journal_mode = WAL');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        seq INTEGER NOT NULL,
        timestamp TEXT NOT NULL,
        source TEXT NOT NULL,
        event TEXT NOT NULL,
        project_id TEXT NOT NULL,
        run_id TEXT,
        node_id TEXT,
        work_item_id TEXT,
        details TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_events_seq ON events(seq);
      CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id);
      CREATE INDEX IF NOT EXISTS idx_events_work_item ON events(work_item_id);
    `);
    const row = this.db.prepare('SELECT MAX(seq) AS m FROM events').get() as { m: number | null };
    this.seq = row.m ?? 0;
  }

  append(input: Omit<HarnessEvent, 'id' | 'seq' | 'timestamp'>): HarnessEvent {
    const ev: HarnessEvent = {
      id: crypto.randomUUID(),
      seq: ++this.seq,
      timestamp: new Date().toISOString(),
      ...input,
    };
    this.db
      .prepare(
        `INSERT INTO events (id, seq, timestamp, source, event, project_id, run_id, node_id, work_item_id, details)
         VALUES (@id, @seq, @timestamp, @source, @event, @projectId, @runId, @nodeId, @workItemId, @details)`,
      )
      .run({
        id: ev.id,
        seq: ev.seq,
        timestamp: ev.timestamp,
        source: ev.source,
        event: ev.event,
        projectId: ev.projectId,
        runId: ev.runId ?? null,
        nodeId: ev.nodeId ?? null,
        workItemId: ev.workItemId ?? null,
        details: JSON.stringify(ev.details ?? {}),
      });
    return ev;
  }

  listAfter(seq: number, limit = 1000): HarnessEvent[] {
    const rows = this.db
      .prepare('SELECT * FROM events WHERE seq > ? ORDER BY seq ASC LIMIT ?')
      .all(seq, limit) as RawRow[];
    return rows.map(mapRow);
  }

  listRecent(limit = 200): HarnessEvent[] {
    const rows = this.db
      .prepare('SELECT * FROM events ORDER BY seq DESC LIMIT ?')
      .all(limit) as RawRow[];
    return rows.reverse().map(mapRow);
  }
}
