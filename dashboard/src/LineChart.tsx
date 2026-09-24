import { useEffect, useId, useMemo, useRef, useState } from "react";

export interface Series {
  key: string;
  label: string;
  color: string; // a CSS custom property, e.g. "var(--series-1)"
  values: number[];
}

interface Props {
  title: string;
  subtitle?: string;
  times: number[]; // unix seconds, one per point
  series: Series[];
  format: (v: number) => string;
  reference?: { value: number; label: string };
  height?: number;
}

const MARGIN = { top: 12, right: 88, bottom: 24, left: 44 };

/** A round tick step (1, 2, 2.5 or 5 x 10^n) giving about four intervals up to `max`. */
function niceStep(max: number): number {
  if (max <= 0) return 0.25;
  const raw = max / 4;
  const exp = 10 ** Math.floor(Math.log10(raw));
  for (const m of [1, 2, 2.5, 5, 10]) {
    if (raw <= m * exp) return m * exp;
  }
  return 10 * exp;
}

function clock(ts: number): string {
  return new Date(ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

/** Time-series line chart: 2px lines, hairline grid, one y-axis, crosshair with a tooltip for every series. */
export function LineChart({ title, subtitle, times, series, format, reference, height = 220 }: Props) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(600);
  const [hover, setHover] = useState<number | null>(null);
  const titleId = useId();

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const observer = new ResizeObserver(([entry]) => setWidth(Math.max(280, entry.contentRect.width)));
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const n = times.length;
  const innerW = width - MARGIN.left - MARGIN.right;
  const innerH = height - MARGIN.top - MARGIN.bottom;
  const { yMax, yTicks } = useMemo(() => {
    const peak = Math.max(0, ...series.flatMap((s) => s.values), reference?.value ?? 0);
    const step = niceStep(peak * 1.05);
    const top = Math.max(step, Math.ceil((peak * 1.05) / step) * step);
    const ticks = Array.from({ length: Math.round(top / step) + 1 }, (_, i) => i * step);
    return { yMax: top, yTicks: ticks };
  }, [series, reference]);

  const x = (i: number) => MARGIN.left + (n <= 1 ? innerW : (i / (n - 1)) * innerW);
  const y = (v: number) => MARGIN.top + innerH - (v / yMax) * innerH;
  const xTicks = n > 1 ? [0, Math.floor((n - 1) / 2), n - 1] : [];

  const path = (values: number[]) =>
    values.map((v, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join("");

  // end labels only when they would not collide (otherwise the legend carries identity)
  const ends = series.map((s) => ({ s, y: y(s.values[n - 1] ?? 0) })).sort((a, b) => a.y - b.y);
  const endLabels = series.length <= 4 && ends.every((e, i) => i === 0 || e.y - ends[i - 1].y >= 14);

  function onPointer(event: React.PointerEvent<SVGRectElement>) {
    if (n === 0) return;
    const rect = event.currentTarget.getBoundingClientRect();
    const fraction = (event.clientX - rect.left) / rect.width;
    setHover(Math.min(n - 1, Math.max(0, Math.round(fraction * (n - 1)))));
  }

  function onKey(event: React.KeyboardEvent) {
    if (n === 0) return;
    if (event.key === "ArrowLeft") setHover((h) => Math.max(0, (h ?? n - 1) - 1));
    else if (event.key === "ArrowRight") setHover((h) => Math.min(n - 1, (h ?? n - 1) + 1));
    else if (event.key === "Escape") setHover(null);
    else return;
    event.preventDefault();
  }

  const tipLeft = hover !== null && x(hover) > width - 180;

  return (
    <figure className="card chart" aria-labelledby={titleId}>
      <figcaption>
        <h3 id={titleId}>{title}</h3>
        {subtitle && <p className="muted">{subtitle}</p>}
      </figcaption>
      {series.length > 1 && (
        <ul className="legend">
          {series.map((s) => (
            <li key={s.key}>
              <span className="line-key" style={{ background: s.color }} />
              {s.label}
            </li>
          ))}
        </ul>
      )}
      <div className="plot" ref={wrapRef}>
        {n === 0 ? (
          <p className="empty muted">Collecting data…</p>
        ) : (
          <svg width={width} height={height} role="img" tabIndex={0} onKeyDown={onKey} onBlur={() => setHover(null)}
               aria-label={`${title}. Use the arrow keys to read values.`}>
            {yTicks.map((t) => (
              <g key={t}>
                <line className="grid" x1={MARGIN.left} x2={MARGIN.left + innerW} y1={y(t)} y2={y(t)} />
                <text className="tick" x={MARGIN.left - 6} y={y(t)} dy="0.32em" textAnchor="end">{format(t)}</text>
              </g>
            ))}
            {xTicks.map((i) => (
              <text key={i} className="tick" x={x(i)} y={height - 6}
                    textAnchor={i === 0 ? "start" : i === n - 1 ? "end" : "middle"}>{clock(times[i])}</text>
            ))}
            {reference && (
              <g>
                <line className="reference" x1={MARGIN.left} x2={MARGIN.left + innerW} y1={y(reference.value)} y2={y(reference.value)} />
                <text className="tick" x={MARGIN.left + innerW + 6} y={y(reference.value)} dy="0.32em">{reference.label}</text>
              </g>
            )}
            {series.map((s) => (
              <path key={s.key} d={path(s.values)} fill="none" stroke={s.color} strokeWidth={2}
                    strokeLinejoin="round" strokeLinecap="round" />
            ))}
            {series.map((s) => (
              <circle key={s.key} className="end-dot" cx={x(n - 1)} cy={y(s.values[n - 1])} r={4} fill={s.color} />
            ))}
            {endLabels && series.map((s) => (
              <text key={s.key} className="end-label" x={x(n - 1) + 10} y={y(s.values[n - 1])} dy="0.32em">
                {s.label} {format(s.values[n - 1])}
              </text>
            ))}
            {hover !== null && (
              <g>
                <line className="crosshair" x1={x(hover)} x2={x(hover)} y1={MARGIN.top} y2={MARGIN.top + innerH} />
                {series.map((s) => (
                  <circle key={s.key} className="end-dot" cx={x(hover)} cy={y(s.values[hover])} r={4} fill={s.color} />
                ))}
              </g>
            )}
            <rect x={MARGIN.left} y={MARGIN.top} width={innerW} height={innerH} fill="transparent"
                  onPointerMove={onPointer} onPointerLeave={() => setHover(null)} />
          </svg>
        )}
        {hover !== null && (
          <div className="tooltip" role="status"
               style={{ left: tipLeft ? undefined : x(hover) + 12, right: tipLeft ? width - x(hover) + 12 : undefined }}>
            <div className="muted">{clock(times[hover])}</div>
            {series.map((s) => (
              <div key={s.key} className="tip-row">
                <span className="line-key" style={{ background: s.color }} />
                <strong>{format(s.values[hover])}</strong>
                <span className="muted">{s.label}</span>
              </div>
            ))}
          </div>
        )}
      </div>
      {n > 0 && (
        <details className="table-view">
          <summary>Table view</summary>
          <table>
            <thead>
              <tr><th>Time</th>{series.map((s) => <th key={s.key}>{s.label}</th>)}</tr>
            </thead>
            <tbody>
              {times.slice(-10).reverse().map((t, j) => {
                const i = n - 1 - j;
                return (
                  <tr key={t}><td>{clock(t)}</td>{series.map((s) => <td key={s.key}>{format(s.values[i])}</td>)}</tr>
                );
              })}
            </tbody>
          </table>
        </details>
      )}
    </figure>
  );
}
