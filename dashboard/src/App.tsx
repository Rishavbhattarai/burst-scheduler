import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, api, getToken, setToken, type Backend, type Job, type Mode, type Policy, type Sample,
  type Stats, type Worker } from "./api";
import { LineChart } from "./LineChart";

const POLL_MS = 2000;

const seconds = (s: number | null | undefined) =>
  s == null ? "–" : s < 60 ? `${s.toFixed(s < 10 ? 1 : 0)} s` : s < 3600 ? `${(s / 60).toFixed(1)} min` : `${(s / 3600).toFixed(1)} h`;
// amounts under a cent get 4 decimals so they do not show as $0.00
const dollars = (v: number | null | undefined, digits = 4) =>
  v == null ? "–" : `$${v.toFixed(v > 0 && v < 0.01 ? 4 : digits)}`;
const count = (v: number) => (Number.isInteger(v) ? String(v) : v.toFixed(1));

export default function App() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [history, setHistory] = useState<Sample[]>([]);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [backends, setBackends] = useState<Backend[]>([]);
  const [policy, setPolicy] = useState<Policy | null>(null);
  const [error, setError] = useState<ApiError | Error | null>(null);
  const [loading, setLoading] = useState(false);
  const lastTs = useRef(0);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const [s, h, j, w, b] = await Promise.all([
        api.stats(), api.history(lastTs.current), api.jobs(50), api.workers(), api.backends(),
      ]);
      setStats(s);
      setJobs(j);
      setWorkers(w);
      setBackends(b);
      if (h.length) {
        lastTs.current = h[h.length - 1].ts;
        setHistory((prev) => [...prev, ...h].slice(-900));
      }
      setError(null);
    } catch (e) {
      setError(e as Error); // keep the last render on screen
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    api.policy().then(setPolicy).catch(() => {});
    const timer = setInterval(refresh, POLL_MS);
    return () => clearInterval(timer);
  }, [refresh]);

  const needsToken = error instanceof ApiError && error.status === 401;
  const times = history.map((h) => h.ts);

  return (
    <div className="app">
      <header className="top">
        <div>
          <h1>Burst scheduler</h1>
          <p className="muted">Local workers first, cloud when the wait gets too long</p>
        </div>
        <Connection error={error} />
      </header>

      {needsToken && <TokenForm onSaved={() => { refresh(); api.policy().then(setPolicy).catch(() => {}); }} />}

      <div className="toolbar">
        <DemoJobs onSubmitted={refresh} />
      </div>

      <main className={loading && stats ? "refreshing" : undefined}>
        {stats && <Tiles stats={stats} />}

        <section className="charts">
          <LineChart
            title="Jobs over time"
            subtitle="Waiting in the queue, running on local workers, running in the cloud"
            times={times}
            format={count}
            series={[
              { key: "queued", label: "Queued", color: "var(--series-1)", values: history.map((h) => h.queued) },
              { key: "local", label: "Local", color: "var(--series-2)", values: history.map((h) => h.local) },
              { key: "cloud", label: "Cloud", color: "var(--series-3)", values: history.map((h) => h.cloud) },
            ]}
          />
          <SpendChart history={history} times={times} cap={policy?.max_spend_per_hour} />
        </section>

        <section className="panels">
          {policy && <PolicyPanel policy={policy} onSaved={setPolicy} />}
          <Capacity workers={workers} backends={backends} />
        </section>

        <JobsTable jobs={jobs} onChanged={refresh} />
      </main>
    </div>
  );
}

function SpendChart({ history, times, cap }: { history: Sample[]; times: number[]; cap?: number }) {
  const values = history.map((h) => h.spend_rate_per_hour);
  const peak = Math.max(0, ...values);
  // draw the cap only when spending gets near it; otherwise it would flatten the line against the axis
  const showCap = cap !== undefined && peak >= cap * 0.4;
  return (
    <LineChart
      title="Cloud spend rate"
      subtitle={`Dollars per hour for the cloud jobs running now${cap !== undefined && !showCap ? ` (cap $${cap}/h)` : ""}`}
      times={times}
      format={(v) => `$${v.toFixed(2)}`}
      series={[{ key: "spend", label: "Spend rate", color: "var(--series-1)", values }]}
      reference={showCap ? { value: cap, label: `cap $${cap}/h` } : undefined}
    />
  );
}

function Connection({ error }: { error: Error | null }) {
  return error ? (
    <p className="status status-critical" role="alert"><span aria-hidden>✕</span> {error.message || "controller unreachable"}</p>
  ) : (
    <p className="status status-good"><span aria-hidden>●</span> Live, every {POLL_MS / 1000} s</p>
  );
}

function TokenForm({ onSaved }: { onSaved: () => void }) {
  const [value, setValue] = useState(getToken());
  return (
    <form className="card token" onSubmit={(e) => { e.preventDefault(); setToken(value); onSaved(); }}>
      <label>API token (BURST_API_TOKEN)
        <input type="password" value={value} onChange={(e) => setValue(e.target.value)} autoComplete="off" />
      </label>
      <button type="submit">Save</button>
    </form>
  );
}

function Tiles({ stats }: { stats: Stats }) {
  const { cloud } = stats;
  const budgetUsed = cloud.daily_budget > 0 ? Math.min(1, cloud.spent_today / cloud.daily_budget) : 0;
  const meterLevel = budgetUsed >= 0.9 ? "critical" : budgetUsed >= 0.7 ? "warning" : "ok";
  return (
    <section className="tiles" aria-label="Summary">
      <Tile label="Queued" value={String(stats.queue_depth)} note={`oldest waiting ${seconds(stats.oldest_wait_s)}`} />
      <Tile label="Wait, last 5 min" value={seconds(stats.wait_p50_s)} note={`p95 ${seconds(stats.wait_p95_s)}`} />
      <Tile label="Local slots free" value={`${stats.slots_free} / ${stats.slots_total}`}
            note={`${stats.workers} worker${stats.workers === 1 ? "" : "s"} · new job waits ${seconds(stats.estimated_wait_s)}`} />
      <Tile label="Cloud jobs" value={String(cloud.active)}
            note={Object.entries(cloud.active_by_backend).map(([b, n]) => `${b} ${n}`).join(" · ") || "none running"} />
      <div className="card tile">
        <p className="tile-label">Cloud spend today</p>
        <p className="tile-value">{dollars(cloud.spent_today, 2)}</p>
        <div className={`meter meter-${meterLevel}`} role="meter" aria-valuemin={0} aria-valuemax={cloud.daily_budget}
             aria-valuenow={cloud.spent_today} aria-label="Cloud spend against the daily budget">
          <span style={{ width: `${budgetUsed * 100}%` }} />
        </div>
        <p className="muted">of {dollars(cloud.daily_budget, 0)} daily budget · {dollars(cloud.spend_rate_per_hour, 2)}/h now</p>
      </div>
    </section>
  );
}

function Tile({ label, value, note }: { label: string; value: string; note: string }) {
  return (
    <div className="card tile">
      <p className="tile-label">{label}</p>
      <p className="tile-value">{value}</p>
      <p className="muted">{note}</p>
    </div>
  );
}

const MODES: { value: Mode; label: string; help: string }[] = [
  { value: "cheapest", label: "Cheapest", help: "only burst to meet deadlines" },
  { value: "balanced", label: "Balanced", help: "burst when saved time is worth more than it costs" },
  { value: "fastest", label: "Fastest", help: "burst whenever the wait passes the threshold" },
];

function PolicyPanel({ policy, onSaved }: { policy: Policy; onSaved: (p: Policy) => void }) {
  const [draft, setDraft] = useState(policy);
  const [message, setMessage] = useState<{ ok: boolean; text: string } | null>(null);
  useEffect(() => setDraft(policy), [policy]);

  const num = (key: keyof Policy) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setDraft({ ...draft, [key]: e.target.value === "" ? 0 : Number(e.target.value) });

  async function save(e: React.FormEvent) {
    e.preventDefault();
    try {
      const saved = await api.updatePolicy({
        enabled: draft.enabled, mode: draft.mode, burst_threshold_s: draft.burst_threshold_s,
        max_cloud_jobs: draft.max_cloud_jobs, max_spend_per_hour: draft.max_spend_per_hour, daily_budget: draft.daily_budget,
      });
      onSaved(saved);
      setMessage({ ok: true, text: "Saved. The next scheduling pass uses it." });
    } catch (err) {
      setMessage({ ok: false, text: (err as Error).message });
    }
  }

  return (
    <form className="card policy" onSubmit={save}>
      <h3>Burst policy</h3>
      <label className="check">
        <input type="checkbox" checked={draft.enabled} onChange={(e) => setDraft({ ...draft, enabled: e.target.checked })} />
        Bursting enabled
      </label>
      <fieldset>
        <legend>Cost vs speed</legend>
        {MODES.map((m) => (
          <label key={m.value} className="radio">
            <input type="radio" name="mode" value={m.value} checked={draft.mode === m.value}
                   onChange={() => setDraft({ ...draft, mode: m.value })} />
            <span><strong>{m.label}</strong> <span className="muted">{m.help}</span></span>
          </label>
        ))}
      </fieldset>
      <div className="fields">
        <label>Burst after (s)<input type="number" min={0} step={5} value={draft.burst_threshold_s} onChange={num("burst_threshold_s")} /></label>
        <label>Max cloud jobs<input type="number" min={0} value={draft.max_cloud_jobs} onChange={num("max_cloud_jobs")} /></label>
        <label>Spend cap ($/h)<input type="number" min={0} step={0.5} value={draft.max_spend_per_hour} onChange={num("max_spend_per_hour")} /></label>
        <label>Daily budget ($)<input type="number" min={0} step={1} value={draft.daily_budget} onChange={num("daily_budget")} /></label>
      </div>
      <div className="row">
        <button type="submit">Save policy</button>
        {message && <span className={message.ok ? "status-good" : "status-critical"} role="status">{message.text}</span>}
      </div>
    </form>
  );
}

function Capacity({ workers, backends }: { workers: Worker[]; backends: Backend[] }) {
  return (
    <div className="card capacity">
      <h3>Local workers</h3>
      {workers.length === 0 ? <p className="muted">No workers connected.</p> : (
        <table>
          <thead><tr><th>Worker</th><th>Busy</th><th>Status</th></tr></thead>
          <tbody>
            {workers.map((w) => (
              <tr key={w.id}>
                <td className="mono">{w.id}</td>
                <td className="num">{w.running.length} / {w.slots}</td>
                <td>{w.alive ? <span className="status-good">● alive</span> : <span className="status-critical">✕ silent</span>}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <h3>Cloud backends</h3>
      {backends.length === 0 ? <p className="muted">None configured: everything runs locally.</p> : (
        <table>
          <thead><tr><th>Backend</th><th>Running</th><th>Start-up</th><th>Price</th></tr></thead>
          <tbody>
            {backends.map((b) => (
              <tr key={b.name}>
                <td>{b.name}{b.kind !== b.name && <span className="muted"> {b.kind}</span>}{b.cooling_down && <span className="status-warning"> ⚠ paused after an error</span>}</td>
                <td className="num">{b.active} / {b.max_jobs}</td>
                <td className="num">{seconds(b.startup_s)}</td>
                <td className="num">${b.cpu_hour}/vCPU·h</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

const STATE_ICON: Record<string, string> = {
  queued: "◌", dispatched: "↗", running: "▶", succeeded: "✓", failed: "✕", cancelled: "⊘",
};

function JobsTable({ jobs, onChanged }: { jobs: Job[]; onChanged: () => void }) {
  const now = Date.now() / 1000;
  return (
    <section className="card jobs">
      <h3>Recent jobs</h3>
      {jobs.length === 0 ? <p className="muted">No jobs yet. Submit some with the demo form above.</p> : (
        <div className="scroll">
          <table>
            <thead>
              <tr><th>Job</th><th>State</th><th>Where</th><th className="num">Waited</th><th className="num">Cost</th><th>Why</th><th /></tr>
            </thead>
            <tbody>
              {jobs.map((j) => {
                const waited = (j.started_at ?? (j.state === "queued" || j.state === "dispatched" ? now : null));
                return (
                  <tr key={j.id}>
                    <td><span>{j.name}</span> <span className="muted mono">{j.id}</span></td>
                    <td className={`state state-${j.state}`}><span aria-hidden>{STATE_ICON[j.state]}</span> {j.state}</td>
                    <td>{j.backend ?? "–"}</td>
                    <td className="num">{waited == null ? "–" : seconds(waited - j.submitted_at)}</td>
                    <td className="num">{j.backend && j.backend !== "local" ? dollars(j.cost ?? j.cost_estimate) : "–"}</td>
                    <td className="why">{j.error ? `${j.decision ?? ""} · ${j.error}` : j.decision ?? ""}</td>
                    <td>
                      {["queued", "dispatched", "running"].includes(j.state) && (
                        <button type="button" className="ghost" onClick={() => api.cancel(j.id).then(onChanged).catch(() => {})}>
                          Cancel
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function DemoJobs({ onSubmitted }: { onSubmitted: () => void }) {
  const [n, setN] = useState(12);
  const [duration, setDuration] = useState(20);
  const [busy, setBusy] = useState(false);
  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    try {
      for (let i = 0; i < n; i++) {
        await api.submit({ name: `demo-${i + 1}`, command: ["sleep", String(duration)] });
      }
      onSubmitted();
    } finally {
      setBusy(false);
    }
  }
  return (
    <form className="demo" onSubmit={submit}>
      <label>Jobs<input type="number" min={1} max={200} value={n} onChange={(e) => setN(Number(e.target.value))} /></label>
      <label>Seconds each<input type="number" min={1} max={3600} value={duration} onChange={(e) => setDuration(Number(e.target.value))} /></label>
      <button type="submit" disabled={busy}>{busy ? "Submitting…" : "Submit demo jobs"}</button>
    </form>
  );
}
