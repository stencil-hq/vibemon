// Crisp SVG time series: straight segments, dithered area under the lead
// series, square bars, and a hover readout. Sized to its container.

import { type PointerEvent, useEffect, useId, useRef, useState } from "react";
import { fmt } from "./format.ts";

export interface Series {
  label: string;
  values: number[];
  /** `lead` gets the dithered area; `ref` is a dashed reference line (capacity). */
  style: "lead" | "alt" | "ref";
}

interface ChartProps {
  label: string;
  times: number[];
  series: Series[];
  format: (value: number) => string;
  /** Visible time window (ms); defaults to the span of `times`. */
  domain?: [number, number];
  /** Bars of this width (ms) starting at each time, instead of lines. */
  bucketMs?: number;
  /** Values are in a power-of-1024 unit (bytes, MiB): pick binary gridlines. */
  binary?: boolean;
  height?: number;
  empty?: string;
}

const PAD = { top: 12, right: 12, bottom: 24, left: 64 };

/** Smallest 1/2/5×10ⁿ ceiling at or above `value`. */
function niceCeil(value: number): number {
  if (value <= 0) return 1;
  const exp = 10 ** Math.floor(Math.log10(value));
  const unit = value / exp;
  return (unit <= 1 ? 1 : unit <= 2 ? 2 : unit <= 5 ? 5 : 10) * exp;
}

/** Axis ceiling: decimal nice steps, or nice steps of the largest 1024ⁿ unit. */
function axisTop(peak: number, binary: boolean): number {
  if (!binary || peak < 1024) return niceCeil(peak);
  const unit = 1024 ** Math.floor(Math.log(peak) / Math.log(1024));
  return niceCeil(peak / unit) * unit;
}

export function Chart({
  label,
  times,
  series,
  format,
  domain,
  bucketMs,
  binary = false,
  height = 180,
  empty,
}: ChartProps) {
  const wrap = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(600);
  const [hover, setHover] = useState<number | null>(null);
  const patternId = useId().replace(/:/g, "");

  useEffect(() => {
    const element = wrap.current;
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => {
      if (entry) setWidth(Math.max(200, Math.floor(entry.contentRect.width)));
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const n = times.length;
  const plotW = width - PAD.left - PAD.right;
  const plotH = height - PAD.top - PAD.bottom;
  const baseY = PAD.top + plotH;
  const top = axisTop(Math.max(0, ...series.flatMap((s) => s.values)) * 1.05, binary);
  const [start, end] = domain ?? [times[0] ?? 0, (times[n - 1] ?? 0) + (bucketMs ?? 0)];
  const span = Math.max(1, end - start);
  const xAt = (t: number) => PAD.left + ((t - start) / span) * plotW;
  const x = (i: number) => xAt((times[i] ?? start) + (bucketMs ?? 0) / 2);
  const y = (v: number) => PAD.top + plotH - (v / top) * plotH;
  const timeLabel = span < 10 * 60_000 ? fmt.time : fmt.clock;

  const onMove = (event: PointerEvent<SVGRectElement>) => {
    if (n === 0) return;
    const px = event.clientX - event.currentTarget.getBoundingClientRect().left + PAD.left;
    let best = 0;
    for (let i = 1; i < n; i += 1) {
      if (Math.abs(x(i) - px) < Math.abs(x(best) - px)) best = i;
    }
    setHover(best);
  };

  const gridLabels = [0, 0.5, 1].map((r) => ({ r, text: format(top * r) }));
  const ticks = [0, 1 / 3, 2 / 3, 1].map((r) => start + r * span);
  const hoverTime = hover === null ? undefined : times[hover];
  const barW = bucketMs ? Math.max(1, Math.min(24, (bucketMs / span) * plotW * 0.7)) : 0;

  return (
    <div className="chart" ref={wrap}>
      <svg width={width} height={height} role="img" aria-label={label} className="chart__svg">
        <defs>
          <pattern id={patternId} width="3" height="3" patternUnits="userSpaceOnUse">
            <rect width="1" height="1" className="chart__dither" />
            <rect x="1.5" y="1.5" width="1" height="1" className="chart__dither" />
          </pattern>
        </defs>
        {gridLabels.map(({ r, text }, i) => (
          <g key={r}>
            <line
              className={r === 0 ? "chart__axis" : "chart__grid"}
              x1={PAD.left}
              x2={PAD.left + plotW}
              y1={y(top * r)}
              y2={y(top * r)}
            />
            {/* Integer formats can repeat a label (0.5 → "1"); show each once. */}
            {gridLabels.findIndex((g) => g.text === text) === i && (
              <text className="chart__label" x={PAD.left - 8} y={y(top * r) + 4} textAnchor="end">
                {text}
              </text>
            )}
          </g>
        ))}
        {ticks.map((t, i) => (
          <text
            key={t}
            className="chart__label"
            x={xAt(t)}
            y={height - 6}
            textAnchor={i === 0 ? "start" : i === ticks.length - 1 ? "end" : "middle"}
          >
            {timeLabel(t)}
          </text>
        ))}
        {n === 0 && (
          <text
            className="chart__label"
            x={PAD.left + plotW / 2}
            y={PAD.top + plotH / 2}
            textAnchor="middle"
          >
            {empty ?? "No samples yet"}
          </text>
        )}
        {bucketMs
          ? series.map((s) =>
              s.values.map((v, i) => {
                const h = v > 0 ? Math.max(1, baseY - y(v)) : 0;
                return (
                  <rect
                    key={`${s.label}-${times[i]}`}
                    className={`chart__bar chart__bar--${s.style}${hover === i ? " is-hover" : ""}`}
                    x={x(i) - barW / 2}
                    y={baseY - h}
                    width={barW}
                    height={h}
                  />
                );
              }),
            )
          : series.map((s) => {
              if (n === 0) return null;
              const line = s.values
                .map((v, i) => `${i ? "L" : "M"}${x(i).toFixed(1)} ${y(v).toFixed(1)}`)
                .join("");
              return (
                <g key={s.label}>
                  {s.style === "lead" && n > 1 && (
                    <path
                      d={`${line}L${x(n - 1).toFixed(1)} ${baseY}L${x(0).toFixed(1)} ${baseY}Z`}
                      fill={`url(#${patternId})`}
                    />
                  )}
                  <path d={line} className={`chart__line chart__line--${s.style}`} />
                </g>
              );
            })}
        {hover !== null && hoverTime !== undefined && (
          <g>
            <line className="chart__cursor" x1={x(hover)} x2={x(hover)} y1={PAD.top} y2={baseY} />
            {!bucketMs &&
              series.map((s) => (
                <rect
                  key={s.label}
                  className={`chart__point chart__point--${s.style}`}
                  x={x(hover) - 3}
                  y={y(s.values[hover] ?? 0) - 3}
                  width="6"
                  height="6"
                />
              ))}
          </g>
        )}
        <rect
          x={PAD.left}
          y={PAD.top}
          width={plotW}
          height={plotH}
          fill="transparent"
          onPointerMove={onMove}
          onPointerLeave={() => setHover(null)}
        />
      </svg>
      {hover !== null && hoverTime !== undefined && (
        <div
          className="chart__tip"
          style={{ left: Math.min(width - 170, Math.max(0, x(hover) + 10)), top: PAD.top }}
        >
          <div className="chart__tip-time">{fmt.time(hoverTime)}</div>
          {series.map((s) => (
            <div key={s.label} className="chart__tip-row">
              <span className={`chart__key chart__key--${s.style}`} aria-hidden="true" />
              <span className="chart__tip-label">{s.label}</span>
              <span className="chart__tip-value">{format(s.values[hover] ?? 0)}</span>
            </div>
          ))}
        </div>
      )}
      <div className="chart__legend">
        {series.map((s) => (
          <span key={s.label} className="chart__legend-item">
            <span className={`chart__key chart__key--${s.style}`} aria-hidden="true" />
            {s.label}
          </span>
        ))}
      </div>
    </div>
  );
}
