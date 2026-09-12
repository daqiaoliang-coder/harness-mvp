import fs from 'node:fs/promises';
import path from 'node:path';
import YAML from 'yaml';

export interface Template {
  node: string;
  next?: string;
  inputs: string[];
  body: string;
}

export async function loadTemplate(dir: string, nodeKey: string): Promise<Template> {
  const file = path.join(dir, `${nodeKey}.md`);
  const raw = await fs.readFile(file, 'utf8');
  const { meta, body } = parseFrontmatter(raw);
  return {
    node: typeof meta.node === 'string' ? meta.node : nodeKey,
    next: typeof meta.next === 'string' ? meta.next : undefined,
    inputs: Array.isArray(meta.inputs) ? meta.inputs.map(String) : [],
    body,
  };
}

function parseFrontmatter(raw: string): { meta: Record<string, unknown>; body: string } {
  if (!raw.startsWith('---')) return { meta: {}, body: raw };
  const end = raw.indexOf('\n---', 3);
  if (end === -1) return { meta: {}, body: raw };
  const fmText = raw.slice(3, end);
  const body = raw.slice(end + 4).replace(/^\r?\n/, '');
  try {
    const parsed = YAML.parse(fmText);
    return {
      meta: parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : {},
      body,
    };
  } catch {
    return { meta: {}, body: raw };
  }
}

/** 把 {{issue.title}} 这类占位符替换成运行时上下文 */
export function renderPrompt(template: Template, context: Record<string, string>): string {
  let out = template.body;
  for (const [k, v] of Object.entries(context)) {
    out = out.split(`{{${k}}}`).join(v);
  }
  return out;
}
