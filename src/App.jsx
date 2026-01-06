/**
 * Drop-in update: Supabase queries FALL BACK to local CSV rows when Supabase is missing/unreachable.
 *
 * What this adds:
 * 1) computeLocalAggregates now returns `__rows` (cleaned rows from the CSV)
 * 2) A local "query engine" that supports: columns, filters, orderBy, limit
 * 3) Agent prompt includes a new tool: "local_select"
 * 4) If agent returns "supabase" but Supabase is not configured (or errors), we automatically fallback to local CSV.
 *
 * NOTE:
 * - Local queries only work on columns present in the CSV.
 * - Operators supported locally: eq, neq, gt, gte, lt, lte, ilike, in
 */

import React, { useEffect, useMemo, useRef, useState } from "react";
import Papa from "papaparse";
import { GoogleGenerativeAI } from "@google/generative-ai";
import { createClient } from "@supabase/supabase-js";

import {
  ResponsiveContainer,
  LineChart,
  Line,
  AreaChart,
  Area,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from "recharts";

import {
  Bike,
  LayoutDashboard,
  MessageSquare,
  Database,
  Send,
  Pin,
  Trash2,
  Sparkles,
  PlusCircle,
  Table2,
  BarChart3,
  TrendingUp,
  Calendar,
  Route,
  Layers,
  AlertCircle,
  X,
} from "lucide-react";

// -------------------- CONFIG --------------------
const GEMINI_KEY_ENV = import.meta.env.VITE_GEMINI_API_KEY;
const SUPABASE_URL = import.meta.env.VITE_SUPABASE_URL;
const SUPABASE_KEY = import.meta.env.VITE_SUPABASE_ANON_KEY;

const GEMINI_KEY_STORAGE = "dashpilot_gemini_key";

function getGeminiKeyFromStorage() {
  try {
    return localStorage.getItem(GEMINI_KEY_STORAGE) || "";
  } catch {
    return "";
  }
}
function saveGeminiKeyToStorage(k) {
  try {
    localStorage.setItem(GEMINI_KEY_STORAGE, String(k || "").trim());
  } catch {}
}
function clearGeminiKeyFromStorage() {
  try {
    localStorage.removeItem(GEMINI_KEY_STORAGE);
  } catch {}
}

const supabase =
  SUPABASE_URL && SUPABASE_KEY ? createClient(SUPABASE_URL, SUPABASE_KEY) : null;

// -------------------- THEME --------------------
const THEME = {
  bg0: "#070A10",
  bg1: "#0B1020",
  panel: "rgba(17, 24, 39, 0.55)",
  card: "rgba(17, 24, 39, 0.70)",
  border: "rgba(148, 163, 184, 0.18)",
  borderStrong: "rgba(148, 163, 184, 0.28)",
  text: "#EAF2FF",
  muted: "rgba(234, 242, 255, 0.68)",
  accent: "#4DA3FF",
  accent2: "#7C5CFF",
  good: "#2EE59D",
  warn: "#FFB86C",
  bad: "#FF5C7A",
  chart: ["#4DA3FF", "#2EE59D", "#FFB86C", "#FF5C7A", "#A371F7", "#66D9EF"],
};

const shadow = "0 16px 50px rgba(0,0,0,0.38)";
const softShadow = "0 10px 28px rgba(0,0,0,0.28)";

const DOW_SHORT = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const DOW_FULL = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];

function clamp(n, a, b) {
  return Math.min(b, Math.max(a, n));
}
function safeDate(x) {
  const d = new Date(x);
  return Number.isNaN(d.getTime()) ? null : d;
}
function monthKey(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}
function dowIndex(d) {
  return (d.getDay() + 6) % 7;
}
function shortText(s, max = 28) {
  const t = String(s ?? "").trim();
  if (!t) return "Unknown";
  return t.length > max ? t.slice(0, max) + "…" : t;
}
function tryParseJson(s) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}
function stripCodeFences(s) {
  return String(s || "").replace(/```json|```/g, "").trim();
}

// -------------------- LOCAL QUERY ENGINE (NEW) --------------------
function normalizeOperator(op) {
  return String(op || "").toLowerCase().trim();
}

function ilike(haystack, pattern) {
  const hs = String(haystack ?? "");
  const p = String(pattern ?? "");
  // very small "SQL-ish" support: %foo% or foo% or %foo
  const escaped = p.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const reStr = "^" + escaped.replaceAll("%", ".*") + "$";
  try {
    return new RegExp(reStr, "i").test(hs);
  } catch {
    return hs.toLowerCase().includes(p.toLowerCase().replaceAll("%", ""));
  }
}

function applyLocalFilters(rows, filters) {
  const fs = Array.isArray(filters) ? filters : filters ? [filters] : [];
  if (!fs.length) return rows;

  return rows.filter((r) => {
    for (const f of fs) {
      const col = f?.column;
      const op = normalizeOperator(f?.operator);
      const val = f?.value;

      if (!col || !op) continue;

      const cell = r?.[col];

      if (op === "eq") {
        if (cell !== val) return false;
      } else if (op === "neq") {
        if (cell === val) return false;
      } else if (op === "gt") {
        if (!(Number(cell) > Number(val))) return false;
      } else if (op === "gte") {
        if (!(Number(cell) >= Number(val))) return false;
      } else if (op === "lt") {
        if (!(Number(cell) < Number(val))) return false;
      } else if (op === "lte") {
        if (!(Number(cell) <= Number(val))) return false;
      } else if (op === "ilike") {
        if (!ilike(cell, val)) return false;
      } else if (op === "in") {
        const arr = Array.isArray(val) ? val : typeof val === "string" ? val.split(",").map((x) => x.trim()) : [];
        if (!arr.includes(String(cell))) return false;
      } else {
        // unknown operator -> ignore (or you can fail closed)
        continue;
      }
    }
    return true;
  });
}

function applyLocalOrder(rows, orderBy) {
  const col = orderBy?.column;
  if (!col) return rows;

  const asc = !!orderBy?.ascending;
  const sorted = [...rows].sort((a, b) => {
    const av = a?.[col];
    const bv = b?.[col];

    // try date compare if looks like time
    const ad = safeDate(av);
    const bd = safeDate(bv);
    if (ad && bd) return ad - bd;

    // numeric compare if both numeric-ish
    const an = Number(av);
    const bn = Number(bv);
    if (!Number.isNaN(an) && !Number.isNaN(bn)) return an - bn;

    // string compare
    return String(av ?? "").localeCompare(String(bv ?? ""));
  });

  return asc ? sorted : sorted.reverse();
}

function pickColumns(rows, columns) {
  const cols = Array.isArray(columns) && columns.length ? columns : null;
  if (!cols) return rows;
  return rows.map((r) => {
    const out = {};
    for (const c of cols) out[c] = r?.[c];
    return out;
  });
}

async function executeLocalSelect(action, localAgg) {
  if (!localAgg?.__rows?.length) return [];
  const columns = action.columns && Array.isArray(action.columns) ? action.columns : null;

  let rows = localAgg.__rows;

  rows = applyLocalFilters(rows, action.filters);
  rows = applyLocalOrder(rows, action.orderBy);

  const limit = action.limit ? clamp(Number(action.limit) || 10, 1, 500) : 50;
  rows = rows.slice(0, limit);

  rows = pickColumns(rows, columns);

  return rows;
}

// -------------------- WIDGET CATALOG (LOCAL DASHBOARD) --------------------
const WIDGET_CATALOG = [
  {
    id: "w_trips_by_month",
    kind: "chart",
    title: "Trips by Month (Area)",
    icon: TrendingUp,
    build: (local) => ({
      chartType: "area",
      data: local?.tripsByMonth ?? [],
      xKey: "name",
      series: [{ key: "value", label: "Trips" }],
    }),
  },
  {
    id: "w_trips_by_dow",
    kind: "chart",
    title: "Trips by Day of Week (Bar)",
    icon: Calendar,
    build: (local) => ({
      chartType: "bar",
      data: local?.tripsByDOW ?? [],
      xKey: "name",
      series: [{ key: "value", label: "Trips" }],
    }),
  },
  {
    id: "w_duration_dist",
    kind: "chart",
    title: "Duration Distribution (Histogram)",
    icon: BarChart3,
    build: (local) => ({
      chartType: "bar",
      data: local?.durationBuckets ?? [],
      xKey: "name",
      series: [{ key: "value", label: "Trips" }],
    }),
  },
  {
    id: "w_bike_type_split",
    kind: "chart",
    title: "Bike Type Split (Donut)",
    icon: Layers,
    build: (local) => ({
      chartType: "pie",
      data: local?.rideableSplit ?? [],
      donut: true,
      nameKey: "name",
      valueKey: "value",
    }),
  },
  {
    id: "w_top_routes",
    kind: "chart",
    title: "Top Routes (Bar)",
    icon: Route,
    build: (local) => ({
      chartType: "bar",
      data: local?.topRoutes ?? [],
      xKey: "name",
      series: [{ key: "value", label: "Trips" }],
    }),
  },
  {
    id: "w_member_vs_casual_dow",
    kind: "chart",
    title: "Member vs Casual by Day (Stacked)",
    icon: Sparkles,
    build: (local) => ({
      chartType: "stackedBar",
      data: local?.dowMemberCasual ?? [],
      xKey: "name",
      series: [
        { key: "member", label: "Member" },
        { key: "casual", label: "Casual" },
      ],
    }),
  },
  {
    id: "t_latest_local_trips",
    kind: "table",
    title: "Latest Trips (Local CSV)",
    icon: Table2,
    build: (local) => ({
      columns: [
        { key: "started_at", label: "Started" },
        { key: "start_station_name", label: "Start" },
        { key: "end_station_name", label: "End" },
        { key: "member_casual", label: "Rider" },
        { key: "rideable_type", label: "Bike" },
      ],
      rows: local?.latestTrips ?? [],
    }),
  },
  {
    id: "t_top_stations_local",
    kind: "table",
    title: "Top Stations (Local CSV)",
    icon: Table2,
    build: (local) => ({
      columns: [
        { key: "fullName", label: "Station" },
        { key: "value", label: "Trips" },
      ],
      rows: local?.topStationsRaw ?? [],
    }),
  },
];

// -------------------- COMPONENTS --------------------
const IconButton = ({ title, onClick, children, tone = "default" }) => (
  <button
    title={title}
    onClick={onClick}
    style={{
      border: `1px solid ${THEME.border}`,
      background:
        tone === "accent"
          ? "linear-gradient(135deg, rgba(77,163,255,0.20), rgba(124,92,255,0.12))"
          : tone === "danger"
          ? "rgba(255,92,122,0.12)"
          : "rgba(255,255,255,0.04)",
      color: THEME.text,
      cursor: "pointer",
      padding: "8px 10px",
      borderRadius: 12,
      display: "inline-flex",
      alignItems: "center",
      gap: 8,
      transition: "transform .12s ease, border-color .12s ease",
    }}
    onMouseDown={(e) => (e.currentTarget.style.transform = "scale(0.98)")}
    onMouseUp={(e) => (e.currentTarget.style.transform = "scale(1)")}
  >
    {children}
  </button>
);

const NavItem = ({ active, icon, label, onClick }) => (
  <button
    onClick={onClick}
    style={{
      width: "100%",
      display: "flex",
      alignItems: "center",
      gap: 12,
      padding: "12px 14px",
      borderRadius: 14,
      border: `1px solid ${active ? THEME.borderStrong : THEME.border}`,
      background: active
        ? "linear-gradient(135deg, rgba(77,163,255,0.18), rgba(124,92,255,0.10))"
        : "rgba(255,255,255,0.03)",
      color: THEME.text,
      cursor: "pointer",
      fontWeight: 700,
      letterSpacing: 0.2,
    }}
  >
    {icon}
    <span>{label}</span>
  </button>
);

const StatCard = ({ title, value, meta, icon }) => (
  <div
    style={{
      background: THEME.card,
      border: `1px solid ${THEME.border}`,
      borderRadius: 18,
      padding: 18,
      boxShadow: softShadow,
      display: "flex",
      gap: 14,
      alignItems: "flex-start",
      backdropFilter: "blur(14px)",
    }}
  >
    <div
      style={{
        width: 44,
        height: 44,
        borderRadius: 16,
        background: "linear-gradient(135deg, rgba(77,163,255,0.18), rgba(124,92,255,0.10))",
        border: `1px solid ${THEME.border}`,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        flex: "0 0 auto",
      }}
    >
      {icon}
    </div>
    <div style={{ minWidth: 0 }}>
      <div style={{ fontSize: 12, color: THEME.muted, fontWeight: 800, textTransform: "uppercase" }}>{title}</div>
      <div style={{ fontSize: 22, fontWeight: 900, marginTop: 6, color: THEME.text }}>{value}</div>
      {meta ? <div style={{ fontSize: 12, color: THEME.muted, marginTop: 6, lineHeight: 1.25 }}>{meta}</div> : null}
    </div>
  </div>
);

const Panel = ({ title, right, children }) => (
  <div
    style={{
      background: THEME.card,
      border: `1px solid ${THEME.border}`,
      borderRadius: 18,
      padding: 18,
      boxShadow: softShadow,
      backdropFilter: "blur(14px)",
    }}
  >
    <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
      <div style={{ fontWeight: 900, color: THEME.text, fontSize: 15 }}>{title}</div>
      {right}
    </div>
    <div style={{ marginTop: 14 }}>{children}</div>
  </div>
);

function ChartRenderer({ config, height = 280 }) {
  if (!config) return null;
  const { chartType, data, xKey, series, donut, nameKey, valueKey } = config;

  const commonTooltip = (
    <Tooltip
      contentStyle={{
        background: "rgba(10,15,25,0.92)",
        border: `1px solid ${THEME.borderStrong}`,
        borderRadius: 12,
        color: THEME.text,
      }}
      labelStyle={{ color: THEME.muted }}
    />
  );

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer width="100%" height="100%">
        {chartType === "line" ? (
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.15)" />
            <XAxis dataKey={xKey} stroke={THEME.muted} tick={{ fontSize: 12 }} />
            <YAxis stroke={THEME.muted} tick={{ fontSize: 12 }} />
            {commonTooltip}
            <Legend />
            {(series || []).map((s, i) => (
              <Line
                key={s.key}
                type="monotone"
                dataKey={s.key}
                name={s.label}
                stroke={THEME.chart[i % THEME.chart.length]}
                strokeWidth={2.5}
                dot={false}
              />
            ))}
          </LineChart>
        ) : chartType === "area" ? (
          <AreaChart data={data}>
            <defs>
              {(series || []).map((s, i) => (
                <linearGradient key={s.key} id={`g_${s.key}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={THEME.chart[i % THEME.chart.length]} stopOpacity={0.35} />
                  <stop offset="95%" stopColor={THEME.chart[i % THEME.chart.length]} stopOpacity={0.04} />
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.15)" />
            <XAxis dataKey={xKey} stroke={THEME.muted} tick={{ fontSize: 12 }} />
            <YAxis stroke={THEME.muted} tick={{ fontSize: 12 }} />
            {commonTooltip}
            <Legend />
            {(series || []).map((s, i) => (
              <Area
                key={s.key}
                type="monotone"
                dataKey={s.key}
                name={s.label}
                stroke={THEME.chart[i % THEME.chart.length]}
                fill={`url(#g_${s.key})`}
                strokeWidth={2}
                dot={false}
              />
            ))}
          </AreaChart>
        ) : chartType === "stackedBar" ? (
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.15)" />
            <XAxis dataKey={xKey} stroke={THEME.muted} tick={{ fontSize: 12 }} />
            <YAxis stroke={THEME.muted} tick={{ fontSize: 12 }} />
            {commonTooltip}
            <Legend />
            {(series || []).map((s, i) => (
              <Bar
                key={s.key}
                dataKey={s.key}
                name={s.label}
                stackId="a"
                fill={THEME.chart[i % THEME.chart.length]}
                radius={i === series.length - 1 ? [10, 10, 0, 0] : [0, 0, 0, 0]}
              />
            ))}
          </BarChart>
        ) : chartType === "bar" ? (
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.15)" />
            <XAxis
              dataKey={xKey}
              stroke={THEME.muted}
              tick={{ fontSize: 11 }}
              interval={0}
              angle={-16}
              textAnchor="end"
              height={64}
            />
            <YAxis stroke={THEME.muted} tick={{ fontSize: 12 }} />
            {commonTooltip}
            <Legend />
            {(series || []).map((s, i) => (
              <Bar key={s.key} dataKey={s.key} name={s.label} fill={THEME.chart[i % THEME.chart.length]} radius={[10, 10, 0, 0]} />
            ))}
          </BarChart>
        ) : chartType === "pie" ? (
          <PieChart>
            {commonTooltip}
            <Legend />
            <Pie
              data={data}
              dataKey={valueKey || "value"}
              nameKey={nameKey || "name"}
              innerRadius={donut ? 62 : 0}
              outerRadius={96}
              paddingAngle={2}
              stroke="rgba(255,255,255,0.05)"
            >
              {(data || []).map((_, i) => (
                <Cell key={i} fill={THEME.chart[i % THEME.chart.length]} />
              ))}
            </Pie>
          </PieChart>
        ) : null}
      </ResponsiveContainer>
    </div>
  );
}

function TableRenderer({ columns, rows, maxHeight = 320 }) {
  const cols =
    columns && columns.length
      ? columns
      : Object.keys(rows?.[0] || {})
          .slice(0, 6)
          .map((k) => ({ key: k, label: k }));

  return (
    <div style={{ border: `1px solid ${THEME.border}`, borderRadius: 14, overflow: "hidden", background: "rgba(0,0,0,0.18)" }}>
      <div style={{ overflowX: "auto", maxHeight, overflowY: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
          <thead
            style={{
              position: "sticky",
              top: 0,
              background: "rgba(10,15,25,0.92)",
              backdropFilter: "blur(8px)",
              zIndex: 1,
            }}
          >
            <tr>
              {cols.map((c) => (
                <th
                  key={c.key}
                  style={{
                    textAlign: "left",
                    padding: "10px 12px",
                    color: THEME.muted,
                    borderBottom: `1px solid ${THEME.border}`,
                    fontWeight: 900,
                    whiteSpace: "nowrap",
                  }}
                >
                  {c.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(rows || []).map((r, i) => (
              <tr key={i} style={{ borderBottom: `1px solid ${THEME.border}` }}>
                {cols.map((c) => {
                  let v = r?.[c.key];
                  if (c.key.includes("started_at") || c.key.includes("ended_at")) {
                    const d = safeDate(v);
                    v = d ? d.toLocaleString() : v;
                  }
                  return (
                    <td key={c.key} style={{ padding: "10px 12px", color: THEME.text, whiteSpace: "nowrap" }}>
                      {String(v ?? "")}
                    </td>
                  );
                })}
              </tr>
            ))}
            {!rows?.length ? (
              <tr>
                <td colSpan={cols.length} style={{ padding: 12, color: THEME.muted }}>
                  No rows
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// -------------------- MAIN APP --------------------
export default function App() {
  const [activeTab, setActiveTab] = useState("dashboard");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const [local, setLocal] = useState(null);
  const [widgets, setWidgets] = useState([]);

  const [input, setInput] = useState("");
  const [messages, setMessages] = useState([
    {
      role: "assistant",
      type: "text",
      content:
        "I can: (1) query Supabase for rows (if configured), or fallback to local CSV rows, and (2) add NEW dashboard widgets from the local CSV.\n\nType: “show widget menu”.",
    },
  ]);

  const chatEndRef = useRef(null);

  const [geminiKey, setGeminiKey] = useState(() => {
    const fromStorage = getGeminiKeyFromStorage();
    return fromStorage || GEMINI_KEY_ENV || "";
  });
  const [showKeyModal, setShowKeyModal] = useState(false);
  const canUseAI = Boolean(geminiKey);

  // -------------------- LOAD LOCAL CSV --------------------
  useEffect(() => {
    fetch("/trips_rows.csv")
      .then((res) => {
        if (!res.ok) throw new Error("CSV file not found: put trips_rows.csv in /public");
        return res.text();
      })
      .then((csv) => {
        Papa.parse(csv, {
          header: true,
          dynamicTyping: true,
          skipEmptyLines: true,
          complete: (results) => {
            if (!results?.data?.length) {
              setError("CSV parsed but contains no rows.");
              setLoading(false);
              return;
            }
            const agg = computeLocalAggregates(results.data);
            setLocal(agg);

            setMessages((prev) => [
              ...prev,
              {
                role: "assistant",
                type: "options",
                content: {
                  title: "Widget Catalog (Add to Dashboard)",
                  items: WIDGET_CATALOG.map((w) => ({ id: w.id, kind: w.kind, title: w.title })),
                },
              },
            ]);

            setLoading(false);
          },
          error: (e) => {
            setError("CSV parse error: " + (e?.message || String(e)));
            setLoading(false);
          },
        });
      })
      .catch((e) => {
        setError(e?.message || String(e));
        setLoading(false);
      });
  }, []);

  // -------------------- LOCAL AGGREGATION --------------------
  function computeLocalAggregates(rows) {
    const clean = [];
    for (const r of rows) {
      if (!r?.ride_id) continue;
      const s = safeDate(r.started_at);
      const e = safeDate(r.ended_at);
      if (!s || !e) continue;

      const durMin = (e - s) / 60000;
      if (!(durMin > 0 && durMin <= 240)) continue;

      // Keep as plain object (no Date refs), to make local querying simpler
      clean.push({
        ...r,
        __durMin: durMin,
      });
    }

    // keep max N to avoid huge memory on GH Pages
    const MAX_LOCAL_ROWS = 50000;
    const localRows = clean.length > MAX_LOCAL_ROWS ? clean.slice(0, MAX_LOCAL_ROWS) : clean;

    const total = localRows.length;
    const members = localRows.filter((r) => String(r.member_casual).toLowerCase() === "member").length;
    const casual = total - members;

    const hourlyCounts = Array.from({ length: 24 }, (_, h) => ({
      name: `${String(h).padStart(2, "0")}:00`,
      value: 0,
    }));
    for (const r of localRows) {
      const s = safeDate(r.started_at);
      if (!s) continue;
      hourlyCounts[s.getHours()].value += 1;
    }

    const dowCounts = Array.from({ length: 7 }, (_, i) => ({ name: DOW_SHORT[i], value: 0 }));
    const dowMC = Array.from({ length: 7 }, (_, i) => ({ name: DOW_SHORT[i], member: 0, casual: 0 }));
    for (const r of localRows) {
      const s = safeDate(r.started_at);
      if (!s) continue;
      const di = dowIndex(s);
      dowCounts[di].value += 1;
      if (String(r.member_casual).toLowerCase() === "member") dowMC[di].member += 1;
      else dowMC[di].casual += 1;
    }

    const monthMap = new Map();
    for (const r of localRows) {
      const s = safeDate(r.started_at);
      if (!s) continue;
      const mk = monthKey(s);
      monthMap.set(mk, (monthMap.get(mk) || 0) + 1);
    }
    const tripsByMonth = Array.from(monthMap.entries())
      .sort((a, b) => (a[0] < b[0] ? -1 : 1))
      .map(([name, value]) => ({ name, value }));

    const buckets = [
      { label: "0–5", min: 0, max: 5 },
      { label: "5–10", min: 5, max: 10 },
      { label: "10–15", min: 10, max: 15 },
      { label: "15–20", min: 15, max: 20 },
      { label: "20–30", min: 20, max: 30 },
      { label: "30–60", min: 30, max: 60 },
      { label: "60–120", min: 60, max: 120 },
      { label: "120–240", min: 120, max: 240.0001 },
    ];
    const bucketCounts = buckets.map(() => 0);
    for (const r of localRows) {
      const d = Number(r.__durMin);
      for (let i = 0; i < buckets.length; i++) {
        if (d >= buckets[i].min && d < buckets[i].max) {
          bucketCounts[i] += 1;
          break;
        }
      }
    }
    const durationBuckets = buckets.map((b, i) => ({ name: b.label, value: bucketCounts[i] }));

    const rideableMap = new Map();
    for (const r of localRows) {
      const rt = String(r.rideable_type || "unknown").trim() || "unknown";
      rideableMap.set(rt, (rideableMap.get(rt) || 0) + 1);
    }
    const rideableSplit = Array.from(rideableMap.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([name, value]) => ({ name, value }));

    const stationMap = new Map();
    for (const r of localRows) {
      const s = String(r.start_station_name || "Unknown").trim() || "Unknown";
      stationMap.set(s, (stationMap.get(s) || 0) + 1);
    }
    const topStationsRaw = Array.from(stationMap.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 12)
      .map(([fullName, value]) => ({ fullName, value, name: shortText(fullName, 16) }));

    const routeMap = new Map();
    for (const r of localRows) {
      const s = String(r.start_station_name || "Unknown").trim() || "Unknown";
      const e = String(r.end_station_name || "Unknown").trim() || "Unknown";
      const k = `${s} → ${e}`;
      routeMap.set(k, (routeMap.get(k) || 0) + 1);
    }
    const topRoutes = Array.from(routeMap.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 12)
      .map(([fullName, value]) => ({ fullName, value, name: shortText(fullName, 26) }));

    const avgDur = total ? localRows.reduce((acc, r) => acc + Number(r.__durMin || 0), 0) / total : 0;

    let peakHourIdx = 0;
    for (let i = 1; i < 24; i++) if (hourlyCounts[i].value > hourlyCounts[peakHourIdx].value) peakHourIdx = i;

    let busiestIdx = 0;
    for (let i = 1; i < 7; i++) if (dowCounts[i].value > dowCounts[busiestIdx].value) busiestIdx = i;

    const latestTrips = [...localRows]
      .sort((a, b) => (safeDate(b.started_at)?.getTime() || 0) - (safeDate(a.started_at)?.getTime() || 0))
      .slice(0, 12)
      .map((r) => ({
        started_at: r.started_at,
        start_station_name: r.start_station_name,
        end_station_name: r.end_station_name,
        member_casual: r.member_casual,
        rideable_type: r.rideable_type,
      }));

    return {
      __rows: localRows, // NEW: raw-ish clean rows for local querying

      cleanCount: total,
      members,
      casual,
      memberRatio: total ? (members / total) * 100 : 0,
      avgDurationMin: avgDur,
      peakHour: `${String(peakHourIdx).padStart(2, "0")}:00`,
      busiestDay: DOW_FULL[busiestIdx],
      topStation: topStationsRaw[0]?.fullName || "N/A",

      hourly: hourlyCounts,
      tripsByDOW: dowCounts,
      dowMemberCasual: dowMC,
      tripsByMonth,
      durationBuckets,
      rideableSplit,
      topStationsRaw,
      topRoutes,
      riderSplit: [
        { name: "Member", value: members },
        { name: "Casual", value: casual },
      ],
      latestTrips,
    };
  }

  // -------------------- WIDGET OPS --------------------
  function addWidgetFromCatalog(widgetId) {
    if (!local) return;
    const def = WIDGET_CATALOG.find((w) => w.id === widgetId);
    if (!def) return;
    const built = def.build(local);

    const widget = {
      id: `${Date.now()}_${Math.random().toString(16).slice(2)}`,
      source: "local",
      kind: def.kind,
      title: def.title,
      payload: built,
      createdAt: new Date().toISOString(),
    };
    setWidgets((prev) => [widget, ...prev]);
  }

  function previewWidgetInChat(widgetId) {
    if (!local) return;
    const def = WIDGET_CATALOG.find((w) => w.id === widgetId);
    if (!def) return;
    const built = def.build(local);

    setMessages((prev) => [
      ...prev,
      { role: "assistant", type: def.kind, content: { title: def.title, source: "local", payload: built, widgetId } },
    ]);
  }

  function pinChatPayloadToDashboard(message) {
    const m = message;
    if (!m || (m.type !== "chart" && m.type !== "table")) return;

    const widget = {
      id: `${Date.now()}_${Math.random().toString(16).slice(2)}`,
      source: m.content?.source || "custom",
      kind: m.type,
      title: m.content?.title || "Pinned Widget",
      payload: m.content?.payload,
      createdAt: new Date().toISOString(),
    };
    setWidgets((prev) => [widget, ...prev]);
  }

  function removeWidget(id) {
    setWidgets((prev) => prev.filter((w) => w.id !== id));
  }

  // -------------------- SUPABASE EXECUTOR --------------------
  async function executeSupabaseAction(action) {
    if (!supabase) throw new Error("Supabase not configured (missing VITE_SUPABASE_URL or VITE_SUPABASE_ANON_KEY).");
    if (!action?.table) throw new Error("Missing table in Supabase action.");

    const table = action.table;
    const columns = action.columns && Array.isArray(action.columns) ? action.columns.join(",") : "*";

    let q = supabase.from(table).select(columns);

    const filters = Array.isArray(action.filters) ? action.filters : action.filters ? [action.filters] : [];
    for (const f of filters) {
      if (!f?.column || !f?.operator) continue;
      q = q.filter(f.column, f.operator, f.value);
    }

    if (action.orderBy?.column) q = q.order(action.orderBy.column, { ascending: !!action.orderBy.ascending });
    if (action.limit) q = q.limit(clamp(Number(action.limit) || 10, 1, 200));

    const { data, error: dbError } = await q;
    if (dbError) throw dbError;
    return data || [];
  }

  function pushWidgetMenuMessage() {
    setMessages((prev) => [
      ...prev,
      { role: "assistant", type: "options", content: { title: "Widget Catalog (Add to Dashboard)", items: WIDGET_CATALOG.map((w) => ({ id: w.id, kind: w.kind, title: w.title })) } },
    ]);
  }

  // -------------------- CHAT: AI ORCHESTRATION --------------------
  async function handleSend() {
    const q = String(input || "").trim();
    if (!q) return;

    setMessages((prev) => [...prev, { role: "user", type: "text", content: q }]);
    setInput("");

    const ql = q.toLowerCase();
    if (ql.includes("show widget") || ql.includes("widget menu") || ql === "menu" || ql.includes("options")) {
      pushWidgetMenuMessage();
      return;
    }

    if (!canUseAI) {
      setMessages((prev) => [
        ...prev,
        { role: "assistant", type: "text", content: "AI is disabled (Gemini key missing). Click “Add Key” to enable." },
      ]);
      return;
    }

    try {
      const genAI = new GoogleGenerativeAI(geminiKey);
      const model = genAI.getGenerativeModel({ model: "gemini-3-flash-preview" });

      const widgetList = WIDGET_CATALOG.map((w) => ({ id: w.id, kind: w.kind, title: w.title }));

      const prompt = `
You are DashPilot AI Agent. You can:
(A) Use local CSV aggregates to recommend/preview/add widgets.
(B) Query tabular rows. Prefer Supabase if available, otherwise use local CSV rows.

Return ONLY JSON or plain text. No markdown.

Local Widget Catalog:
${JSON.stringify(widgetList)}

Available tools/actions:

1) Add a widget to dashboard:
{ "tool": "add_widget", "widgetId": "w_trips_by_month" }

2) Preview a widget in chat:
{ "tool": "preview_widget", "widgetId": "w_trips_by_month" }

3) Show widget menu:
{ "tool": "show_menu" }

4) Supabase select query (only if Supabase is configured):
{
  "tool": "supabase",
  "table": "trips",
  "action": "select",
  "columns": ["started_at","start_station_name","member_casual"],
  "filters": [{"column":"start_station_name","operator":"ilike","value":"%Grove%"}],
  "orderBy": {"column":"started_at","ascending": false},
  "limit": 10
}

5) Local CSV select query (fallback; uses same schema):
{
  "tool": "local_select",
  "action": "select",
  "columns": ["started_at","start_station_name","member_casual"],
  "filters": [{"column":"start_station_name","operator":"ilike","value":"%Grove%"}],
  "orderBy": {"column":"started_at","ascending": false},
  "limit": 10
}

If user asks "latest trips", use orderBy started_at desc and a limit.
If Supabase is not configured, do NOT return tool="supabase"; use tool="local_select".

User request:
"${q}"
`;

      const res = await model.generateContent(prompt);
      const raw = res?.response?.text?.() ?? "";
      const cleaned = stripCodeFences(raw);
      const asJson = cleaned.startsWith("{") ? tryParseJson(cleaned) : null;

      if (!asJson) {
        setMessages((prev) => [...prev, { role: "assistant", type: "text", content: raw }]);
        return;
      }

      if (asJson.tool === "show_menu") {
        pushWidgetMenuMessage();
        return;
      }

      if (asJson.tool === "add_widget" && asJson.widgetId) {
        addWidgetFromCatalog(asJson.widgetId);
        setMessages((prev) => [...prev, { role: "assistant", type: "text", content: `Added widget: ${asJson.widgetId}` }]);
        return;
      }

      if (asJson.tool === "preview_widget" && asJson.widgetId) {
        previewWidgetInChat(asJson.widgetId);
        return;
      }

      // ---- NEW: local_select tool ----
      if (asJson.tool === "local_select" && asJson.action === "select") {
        setMessages((prev) => [...prev, { role: "assistant", type: "text", content: "Querying local CSV…" }]);
        const data = await executeLocalSelect(asJson, local);

        setMessages((prev) => [
          ...prev,
          {
            role: "assistant",
            type: "table",
            content: {
              title: `Local CSV results (${data.length} rows)`,
              source: "local",
              payload: {
                columns: asJson.columns?.map((c) => ({ key: c, label: c })) || null,
                rows: data,
              },
            },
          },
        ]);
        return;
      }

      // ---- UPDATED: supabase tool with automatic fallback ----
      if (asJson.tool === "supabase" && asJson.action === "select") {
        setMessages((prev) => [...prev, { role: "assistant", type: "text", content: "Querying rows…" }]);

        try {
          const data = await executeSupabaseAction(asJson);

          setMessages((prev) => [
            ...prev,
            {
              role: "assistant",
              type: "table",
              content: {
                title: `Supabase results (${data.length} rows)`,
                source: "supabase",
                payload: {
                  columns: asJson.columns?.map((c) => ({ key: c, label: c })) || null,
                  rows: data,
                },
              },
            },
          ]);
          return;
        } catch (e) {
          // fallback to local
          const localAction = {
            tool: "local_select",
            action: "select",
            columns: asJson.columns,
            filters: asJson.filters,
            orderBy: asJson.orderBy,
            limit: asJson.limit,
          };
          const data = await executeLocalSelect(localAction, local);

          setMessages((prev) => [
            ...prev,
            {
              role: "assistant",
              type: "table",
              content: {
                title: `Fallback to Local CSV (${data.length} rows)`,
                source: "local",
                payload: {
                  columns: localAction.columns?.map((c) => ({ key: c, label: c })) || null,
                  rows: data,
                },
              },
            },
            {
              role: "assistant",
              type: "text",
              content: `Supabase unavailable: ${e?.message || String(e)}. Used local CSV instead.`,
            },
          ]);
          return;
        }
      }

      setMessages((prev) => [...prev, { role: "assistant", type: "text", content: cleaned }]);
    } catch (e) {
      setMessages((prev) => [...prev, { role: "assistant", type: "text", content: `Agent error: ${e?.message || String(e)}` }]);
    }
  }

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, activeTab]);

  // -------------------- BASE DASHBOARD CHARTS --------------------
  const baseCharts = useMemo(() => {
    if (!local) return null;
    return [
      {
        title: "Hourly Trips (Local)",
        kind: "chart",
        payload: { chartType: "line", data: local.hourly, xKey: "name", series: [{ key: "value", label: "Trips" }] },
      },
      {
        title: "Rider Split (Local)",
        kind: "chart",
        payload: { chartType: "pie", data: local.riderSplit, donut: true, nameKey: "name", valueKey: "value" },
      },
    ];
  }, [local]);

  // -------------------- RENDER HELPERS (unchanged from your version) --------------------
  // Keep your existing renderChatMessage + UI layout code as-is.
  // The only functional changes needed were:
  // - local.__rows in aggregates
  // - executeLocalSelect + tool wiring in handleSend
  // - prompt updated to include local_select
  // - supabase fallback

  return (
    <>
      {/* Keep the rest of your UI exactly as you have it.
          Only ensure your "Send" button calls handleSend, and the analyst tab renders messages. */}
      {/* ...YOUR EXISTING UI FROM HERE... */}
      <div />
    </>
  );
}
