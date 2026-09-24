// Types and calls for the controller's REST API.

export type JobState = "queued" | "dispatched" | "running" | "succeeded" | "failed" | "cancelled";

export interface Job {
  id: string;
  name: string;
  command: string[];
  state: JobState;
  priority: number;
  backend: string | null;
  worker: string | null;
  submitted_at: number;
  started_at: number | null;
  finished_at: number | null;
  exit_code: number | null;
  error: string | null;
  cost_estimate: number | null;
  cost: number | null;
  decision: string | null;
}

export interface Stats {
  counts: Record<JobState, number>;
  queue_depth: number;
  oldest_wait_s: number;
  wait_p50_s: number | null;
  wait_p95_s: number | null;
  workers: number;
  slots_total: number;
  slots_free: number;
  estimated_wait_s: number | null;
  cloud: {
    active: number;
    active_by_backend: Record<string, number>;
    spend_rate_per_hour: number;
    spent_today: number;
    daily_budget: number;
  };
}

export interface Sample {
  ts: number;
  queued: number;
  local: number;
  cloud: number;
  spend_rate_per_hour: number;
}

export interface Worker {
  id: string;
  hostname: string;
  slots: number;
  running: string[];
  free: number;
  alive: boolean;
  last_seen: number;
}

export interface Backend {
  name: string;
  kind: string;
  startup_s: number;
  max_jobs: number;
  cpu_hour: number;
  gb_hour: number;
  active: number;
  cooling_down: boolean;
}

export type Mode = "cheapest" | "balanced" | "fastest";

export interface Policy {
  enabled: boolean;
  burst_threshold_s: number;
  mode: Mode;
  value_per_hour: number | null;
  max_cloud_jobs: number;
  max_spend_per_hour: number;
  daily_budget: number;
  default_runtime_s: number;
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

const TOKEN_KEY = "burst-api-token";

export function getToken(): string {
  try {
    return localStorage.getItem(TOKEN_KEY) ?? "";
  } catch {
    return "";
  }
}

export function setToken(token: string): void {
  try {
    localStorage.setItem(TOKEN_KEY, token);
  } catch {
    // private mode: the token only lasts for this page
  }
}

async function call<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (token) headers.authorization = `Bearer ${token}`;
  const response = await fetch(path, { ...init, headers: { ...headers, ...(init.headers ?? {}) } });
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = await response.json();
      detail = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail);
    } catch {
      // not JSON
    }
    throw new ApiError(response.status, detail);
  }
  return response.json() as Promise<T>;
}

export const api = {
  stats: () => call<Stats>("/stats"),
  history: (since: number) => call<Sample[]>(`/stats/history?since=${since}`),
  jobs: (limit = 50) => call<Job[]>(`/jobs?limit=${limit}`),
  workers: () => call<Worker[]>("/workers"),
  backends: () => call<Backend[]>("/backends"),
  policy: () => call<Policy>("/policy"),
  updatePolicy: (update: Partial<Policy>) => call<Policy>("/policy", { method: "PUT", body: JSON.stringify(update) }),
  submit: (job: { name: string; command: string[]; priority?: number }) =>
    call<Job>("/jobs", { method: "POST", body: JSON.stringify(job) }),
  cancel: (id: string) => call<Job>(`/jobs/${id}/cancel`, { method: "POST" }),
};
