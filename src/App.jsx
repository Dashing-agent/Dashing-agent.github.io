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

// IMPORTANT for GitHub Pages + Vite base:
const CSV_URL = `${import.meta.env.BASE_URL || "/"}trips_rows.csv`;

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
  } catch {
    // ignore
  }
}
function clearGeminiKeyFromStorage() {
  try {
    localStorage.removeItem(GEMINI_KEY_STORAGE);
  } catch {
    // ignore
  }
}

const supabase =
  SUPABASE_URL && SUPABASE_KEY ? createClient(SUPABASE_URL, SUPABASE_KEY) : null;

// -------------------- THEME --------------------
const THEME = {
  bg0: "#070A10",
  bg1: "#0B1020",
  panel: "rgba(17, 24, 39, 0.55)", // glass
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
  // JS: Sun=0..Sat=6 -> Mon=0..Sun=6
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

// -------------------- LOCAL QUERY ENGINE (FALLBACK) --------------------
function normalizeOperator(op) {
  return String(op || "").toLowerCase().trim();
}
function ilike(haystack, pattern) {
  const hs = String(haystack ?? "");
  const p = String(pattern ?? "");
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
        const arr = Array.isArray(val)
          ? val.map(String)
          : typeof val === "string"
          ? val.split(",").map((x) => x.trim())
          : [];
        if (!arr.includes(String(cell))) return false;
      } else {
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

    const ad = safeDate(av);
    const bd = safeDate(bv);
    if (ad && bd) return ad - bd;

    const an = Number(av);
    const bn = Number(bv);
    if (!Number.isNaN(an) && !Number.isNaN(bn)) return an - bn;

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
  let rows = localAgg.__rows;

  rows = applyLocalFilters(rows, action.filters);
  rows = applyLocalOrder(rows, action.orderBy);

  const limit = action.limit ? clamp(Number(action.limit) || 10, 1, 500) : 50;
  rows = rows.slice(0, limit);

  rows = pickColumns(rows, action.columns);

  return rows;
}

// -------------------- WIDGET CATALOG (LOCAL DASHBOARD) --------------------
const WIDGET_CATALOG = [
  // Charts
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
  // Tables
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
      <div style={{ fontSize: 12, color: THEME.muted, fontWeight: 800, textTransform: "uppercase" }}>
        {title}
      </div>
      <div style={{ fontSize: 22, fontWeight: 900, marginTop: 6, color: THEME.text }}>
        {value}
      </div>
      {meta ? (
        <div style={{ fontSize: 12, color: THEME.muted, marginTop: 6, lineHeight: 1.25 }}>{meta}</div>
      ) : null}
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
              <Bar
                key={s.key}
                dataKey={s.key}
                name={s.label}
                fill={THEME.chart[i % THEME.chart.length]}
                radius={[10, 10, 0, 0]}
              />
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
      : Object.keys(rows?.[0] || {}).slice(0, 6).map((k) => ({ key: k, label: k }));

  return (
    <div
      style={{
        border: `1px solid ${THEME.border}`,
        borderRadius: 14,
        overflow: "hidden",
        background: "rgba(0,0,0,0.18)",
      }}
    >
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
  const [activeTab, setActiveTab] = useState("dashboard"); // dashboard | analyst
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  // Local dashboard aggregates
  const [local, setLocal] = useState(null);

  // Pinned widgets shown on dashboard (added via chatbot)
  const [widgets, setWidgets] = useState([]);

  // Chat state
  const [input, setInput] = useState("");
  const [messages, setMessages] = useState([
    {
      role: "assistant",
      type: "text",
      content:
        "I can: (1) query Supabase for trip rows (if configured) OR fallback to Local CSV rows, and (2) add NEW dashboard widgets from the local CSV aggregates.\n\nType: “show widget menu” to see available widgets.",
    },
  ]);

  const chatEndRef = useRef(null);

  // ---- Gemini runtime key (BYOK) ----
  const [geminiKey, setGeminiKey] = useState(() => {
    const fromStorage = getGeminiKeyFromStorage();
    return fromStorage || GEMINI_KEY_ENV || "";
  });
  const [showKeyModal, setShowKeyModal] = useState(false);
  const canUseAI = Boolean(geminiKey);

  // -------------------- LOAD LOCAL CSV (Dashboard only) --------------------
  useEffect(() => {
    // Do NOT block local dashboard loading if Gemini is missing.
    fetch(CSV_URL)
      .then((res) => {
        if (!res.ok) throw new Error(`CSV file not found: put trips_rows.csv in /public (fetch: ${CSV_URL})`);
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

            // After local is ready: proactively show widget menu once
            setMessages((prev) => [
              ...prev,
              {
                role: "assistant",
                type: "options",
                content: {
                  title: "Widget Catalog (Add to Dashboard)",
                  items: WIDGET_CATALOG.map((w) => ({
                    id: w.id,
                    kind: w.kind,
                    title: w.title,
                  })),
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

      clean.push({ ...r, __durMin: durMin });
    }

    // Optional cap for GH Pages performance
    const MAX_LOCAL_ROWS = 50000;
    const cleanRows = clean.length > MAX_LOCAL_ROWS ? clean.slice(0, MAX_LOCAL_ROWS) : clean;

    const total = cleanRows.length;
    const members = cleanRows.filter((r) => String(r.member_casual).toLowerCase() === "member").length;
    const casual = total - members;

    // Hourly
    const hourlyCounts = Array.from({ length: 24 }, (_, h) => ({
      name: `${String(h).padStart(2, "0")}:00`,
      value: 0,
    }));
    for (const r of cleanRows) {
      const s = safeDate(r.started_at);
      if (!s) continue;
      hourlyCounts[s.getHours()].value += 1;
    }

    // DOW
    const dowCounts = Array.from({ length: 7 }, (_, i) => ({ name: DOW_SHORT[i], value: 0 }));
    const dowMC = Array.from({ length: 7 }, (_, i) => ({ name: DOW_SHORT[i], member: 0, casual: 0 }));
    for (const r of cleanRows) {
      const s = safeDate(r.started_at);
      if (!s) continue;
      const di = dowIndex(s);
      dowCounts[di].value += 1;
      if (String(r.member_casual).toLowerCase() === "member") dowMC[di].member += 1;
      else dowMC[di].casual += 1;
    }

    // Month
    const monthMap = new Map();
    for (const r of cleanRows) {
      const s = safeDate(r.started_at);
      if (!s) continue;
      const mk = monthKey(s);
      monthMap.set(mk, (monthMap.get(mk) || 0) + 1);
    }
    const tripsByMonth = Array.from(monthMap.entries())
      .sort((a, b) => (a[0] < b[0] ? -1 : 1))
      .map(([name, value]) => ({ name, value }));

    // Duration histogram buckets
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
    for (const r of cleanRows) {
      const d = Number(r.__durMin);
      for (let i = 0; i < buckets.length; i++) {
        if (d >= buckets[i].min && d < buckets[i].max) {
          bucketCounts[i] += 1;
          break;
        }
      }
    }
    const durationBuckets = buckets.map((b, i) => ({ name: b.label, value: bucketCounts[i] }));

    // Rideable split
    const rideableMap = new Map();
    for (const r of cleanRows) {
      const rt = String(r.rideable_type || "unknown").trim() || "unknown";
      rideableMap.set(rt, (rideableMap.get(rt) || 0) + 1);
    }
    const rideableSplit = Array.from(rideableMap.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([name, value]) => ({ name, value }));

    // Top stations
    const stationMap = new Map();
    for (const r of cleanRows) {
      const s = String(r.start_station_name || "Unknown").trim() || "Unknown";
      stationMap.set(s, (stationMap.get(s) || 0) + 1);
    }
    const topStationsRaw = Array.from(stationMap.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 12)
      .map(([fullName, value]) => ({ fullName, value, name: shortText(fullName, 16) }));

    // Top routes
    const routeMap = new Map();
    for (const r of cleanRows) {
      const s = String(r.start_station_name || "Unknown").trim() || "Unknown";
      const e = String(r.end_station_name || "Unknown").trim() || "Unknown";
      const k = `${s} → ${e}`;
      routeMap.set(k, (routeMap.get(k) || 0) + 1);
    }
    const topRoutes = Array.from(routeMap.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 12)
      .map(([fullName, value]) => ({ fullName, value, name: shortText(fullName, 26) }));

    // Avg duration
    const avgDur = total ? cleanRows.reduce((acc, r) => acc + Number(r.__durMin || 0), 0) / total : 0;

    // Peak hour
    let peakHourIdx = 0;
    for (let i = 1; i < 24; i++) if (hourlyCounts[i].value > hourlyCounts[peakHourIdx].value) peakHourIdx = i;

    // Busiest day
    let busiestIdx = 0;
    for (let i = 1; i < 7; i++) if (dowCounts[i].value > dowCounts[busiestIdx].value) busiestIdx = i;

    // Latest trips (local)
    const latestTrips = [...cleanRows]
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
      __rows: cleanRows, // <- NEW: store rows for local fallback queries

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
      {
        role: "assistant",
        type: def.kind,
        content: {
          title: def.title,
          source: "local",
          payload: built,
          widgetId,
        },
      },
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

    if (action.orderBy?.column) {
      q = q.order(action.orderBy.column, { ascending: !!action.orderBy.ascending });
    }
    if (action.limit) q = q.limit(clamp(Number(action.limit) || 10, 1, 200));

    const { data, error: dbError } = await q;
    if (dbError) throw dbError;
    return data || [];
  }

  // -------------------- CHAT: SHOW MENU (NO AI) --------------------
  function pushWidgetMenuMessage() {
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
        {
          role: "assistant",
          type: "text",
          content: "AI is disabled (Gemini key missing). Click “Add Key” in the sidebar to enable.",
        },
      ]);
      return;
    }

    try {
      const genAI = new GoogleGenerativeAI(geminiKey);
      const model = genAI.getGenerativeModel({ model: "gemini-3-flash-preview" });

      const widgetList = WIDGET_CATALOG.map((w) => ({
        id: w.id,
        kind: w.kind,
        title: w.title,
      }));

      const prompt = `
You are DashPilot AI Agent.

You can:
(A) Preview or add NEW widgets to the dashboard (from local CSV aggregates)
(B) Query tabular trip rows.
  - Prefer Supabase when configured.
  - If Supabase is not configured, query Local CSV rows.

Return ONLY JSON or plain text. No markdown.

Local Widget Catalog:
${JSON.stringify(widgetList)}

Tools you may return:

1) Add a widget:
{ "tool": "add_widget", "widgetId": "w_trips_by_month" }

2) Preview a widget:
{ "tool": "preview_widget", "widgetId": "w_trips_by_month" }

3) Show widget menu:
{ "tool": "show_menu" }

4) Query Supabase (if available):
{
  "tool": "supabase",
  "table": "trips",
  "action": "select",
  "columns": ["started_at","start_station_name","member_casual"],
  "filters": [{"column":"start_station_name","operator":"ilike","value":"%Grove%"}],
  "orderBy": {"column":"started_at","ascending": false},
  "limit": 10
}

5) Query Local CSV rows (fallback):
{
  "tool": "local_select",
  "action": "select",
  "columns": ["started_at","start_station_name","member_casual"],
  "filters": [{"column":"start_station_name","operator":"ilike","value":"%Grove%"}],
  "orderBy": {"column":"started_at","ascending": false},
  "limit": 10
}

If user asks for "latest trips", use orderBy started_at desc and a limit.
If Supabase is not configured, DO NOT return tool="supabase"; return tool="local_select".

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
        setMessages((prev) => [
          ...prev,
          { role: "assistant", type: "text", content: `Added widget to dashboard: ${asJson.widgetId}` },
        ]);
        return;
      }

      if (asJson.tool === "preview_widget" && asJson.widgetId) {
        previewWidgetInChat(asJson.widgetId);
        return;
      }

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
          // Automatic fallback to local
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
      setMessages((prev) => [
        ...prev,
        { role: "assistant", type: "text", content: `Agent error: ${e?.message || String(e)}` },
      ]);
    }
  }

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, activeTab]);

  // -------------------- RENDER: CHAT MESSAGE --------------------
  function renderChatMessage(m, idx) {
    const isUser = m.role === "user";

    const bubbleStyle = {
      maxWidth: "86%",
      padding: "14px 14px",
      borderRadius: 18,
      border: `1px solid ${THEME.border}`,
      background: isUser ? "linear-gradient(135deg, rgba(77,163,255,0.22), rgba(124,92,255,0.10))" : THEME.card,
      color: THEME.text,
      boxShadow: isUser ? "none" : "0 12px 36px rgba(0,0,0,0.22)",
      backdropFilter: "blur(12px)",
      whiteSpace: "pre-wrap",
      lineHeight: 1.35,
      fontSize: 14,
    };

    if (m.type === "options") {
      const items = m.content?.items || [];
      return (
        <div key={idx} style={{ display: "flex", justifyContent: "flex-start", marginBottom: 14 }}>
          <div style={{ ...bubbleStyle, maxWidth: "100%" }}>
            <div style={{ fontWeight: 900, marginBottom: 10 }}>{m.content?.title || "Options"}</div>

            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))",
                gap: 10,
              }}
            >
              {items.map((it) => {
                const def = WIDGET_CATALOG.find((w) => w.id === it.id);
                const Icon = def?.icon || PlusCircle;
                return (
                  <div
                    key={it.id}
                    style={{
                      border: `1px solid ${THEME.border}`,
                      borderRadius: 16,
                      padding: 12,
                      background: "rgba(255,255,255,0.03)",
                    }}
                  >
                    <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                      <div
                        style={{
                          width: 36,
                          height: 36,
                          borderRadius: 14,
                          border: `1px solid ${THEME.border}`,
                          background: "rgba(77,163,255,0.12)",
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "center",
                        }}
                      >
                        <Icon size={18} color={THEME.accent} />
                      </div>
                      <div style={{ fontWeight: 900 }}>{it.title}</div>
                    </div>

                    <div style={{ marginTop: 10, display: "flex", gap: 10, flexWrap: "wrap" }}>
                      <IconButton title="Preview in chat" onClick={() => previewWidgetInChat(it.id)}>
                        <Sparkles size={16} />
                        Preview
                      </IconButton>

                      <IconButton
                        title="Add to dashboard"
                        tone="accent"
                        onClick={() => {
                          addWidgetFromCatalog(it.id);
                          setMessages((prev) => [
                            ...prev,
                            { role: "assistant", type: "text", content: `Added to dashboard: ${it.title}` },
                          ]);
                        }}
                      >
                        <PlusCircle size={16} />
                        Add
                      </IconButton>
                    </div>

                    <div style={{ marginTop: 10, color: THEME.muted, fontSize: 12 }}>
                      Command: <span style={{ color: THEME.text }}>add {it.id}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      );
    }

    if (m.type === "chart") {
      const title = m.content?.title || "Chart";
      const cfg = m.content?.payload;
      return (
        <div key={idx} style={{ display: "flex", justifyContent: "flex-start", marginBottom: 14, width: "100%" }}>
          <div style={{ ...bubbleStyle, maxWidth: "100%" }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center" }}>
              <div style={{ fontWeight: 900 }}>{title}</div>
              <IconButton title="Pin to dashboard" tone="accent" onClick={() => pinChatPayloadToDashboard(m)}>
                <Pin size={16} />
                Pin
              </IconButton>
            </div>
            <div style={{ marginTop: 12 }}>
              <ChartRenderer config={cfg} height={260} />
            </div>
          </div>
        </div>
      );
    }

    if (m.type === "table") {
      const title = m.content?.title || "Table";
      const payload = m.content?.payload;
      return (
        <div key={idx} style={{ display: "flex", justifyContent: "flex-start", marginBottom: 14, width: "100%" }}>
          <div style={{ ...bubbleStyle, maxWidth: "100%" }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "center" }}>
              <div style={{ fontWeight: 900 }}>{title}</div>
              <IconButton title="Pin to dashboard" tone="accent" onClick={() => pinChatPayloadToDashboard(m)}>
                <Pin size={16} />
                Pin
              </IconButton>
            </div>
            <div style={{ marginTop: 12 }}>
              <TableRenderer columns={payload?.columns} rows={payload?.rows} />
            </div>
          </div>
        </div>
      );
    }

    return (
      <div
        key={idx}
        style={{
          display: "flex",
          justifyContent: isUser ? "flex-end" : "flex-start",
          marginBottom: 12,
        }}
      >
        <div style={bubbleStyle}>{m.content}</div>
      </div>
    );
  }

  // -------------------- BASE DASHBOARD CHARTS --------------------
  const baseCharts = useMemo(() => {
    if (!local) return null;
    return [
      {
        title: "Hourly Trips (Local)",
        kind: "chart",
        payload: {
          chartType: "line",
          data: local.hourly,
          xKey: "name",
          series: [{ key: "value", label: "Trips" }],
        },
      },
      {
        title: "Rider Split (Local)",
        kind: "chart",
        payload: {
          chartType: "pie",
          data: local.riderSplit,
          donut: true,
          nameKey: "name",
          valueKey: "value",
        },
      },
    ];
  }, [local]);

  // -------------------- UI --------------------
  return (
    <>
      {/* GEMINI KEY MODAL */}
      {showKeyModal ? (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.6)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 9999,
            padding: 16,
          }}
          onClick={() => setShowKeyModal(false)}
        >
          <div
            style={{
              width: "min(720px, 100%)",
              background: THEME.card,
              border: `1px solid ${THEME.border}`,
              borderRadius: 18,
              padding: 16,
              boxShadow: shadow,
              backdropFilter: "blur(14px)",
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }}>
              <div style={{ fontWeight: 1000, fontSize: 16 }}>Set Gemini API Key</div>
              <IconButton title="Close" onClick={() => setShowKeyModal(false)}>
                <X size={16} />
                Close
              </IconButton>
            </div>

            <div style={{ marginTop: 10, color: THEME.muted, fontSize: 13, lineHeight: 1.35 }}>
              Stored in <code>localStorage</code> on this device only. In a public deployment, users can bring their own key.
            </div>

            <div style={{ marginTop: 12, display: "flex", gap: 10, flexWrap: "wrap" }}>
              <input
                value={geminiKey}
                onChange={(e) => setGeminiKey(e.target.value)}
                placeholder="Paste Gemini API key…"
                style={{
                  flex: 1,
                  minWidth: 280,
                  borderRadius: 16,
                  border: `1px solid ${THEME.border}`,
                  background: "rgba(0,0,0,0.20)",
                  color: THEME.text,
                  padding: "12px 14px",
                  outline: "none",
                  fontSize: 14,
                }}
              />
              <IconButton
                title="Save key"
                tone="accent"
                onClick={() => {
                  const trimmed = String(geminiKey || "").trim();
                  saveGeminiKeyToStorage(trimmed);
                  setGeminiKey(trimmed);
                  setShowKeyModal(false);
                  setMessages((prev) => [
                    ...prev,
                    { role: "assistant", type: "text", content: trimmed ? "Gemini key saved. AI enabled." : "Key cleared." },
                  ]);
                }}
              >
                <Sparkles size={16} />
                Save
              </IconButton>
              <IconButton
                title="Clear key"
                tone="danger"
                onClick={() => {
                  clearGeminiKeyFromStorage();
                  setGeminiKey("");
                  setMessages((prev) => [...prev, { role: "assistant", type: "text", content: "Gemini key cleared." }]);
                }}
              >
                <Trash2 size={16} />
                Clear
              </IconButton>
            </div>
          </div>
        </div>
      ) : null}

      <style>{`
        html, body { height: 100%; }
        body {
          margin: 0;
          color: ${THEME.text};
          background: radial-gradient(1000px 700px at 15% -5%, rgba(77,163,255,0.18), transparent 60%),
                      radial-gradient(900px 650px at 85% 0%, rgba(124,92,255,0.16), transparent 55%),
                      linear-gradient(180deg, ${THEME.bg1}, ${THEME.bg0});
          font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
          overflow: hidden;
        }
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 10px; }
        ::-webkit-scrollbar-thumb { background: rgba(148,163,184,0.28); border-radius: 999px; }
      `}</style>

      <div style={{ display: "flex", height: "100vh", width: "100vw" }}>
        {/* SIDEBAR */}
        <aside
          style={{
            width: 290,
            padding: 18,
            borderRight: `1px solid ${THEME.border}`,
            background: "rgba(0,0,0,0.18)",
            backdropFilter: "blur(16px)",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 12,
              padding: 14,
              borderRadius: 18,
              border: `1px solid ${THEME.border}`,
              background: THEME.panel,
              boxShadow: shadow,
            }}
          >
            <div
              style={{
                width: 48,
                height: 48,
                borderRadius: 18,
                border: `1px solid ${THEME.border}`,
                background: "linear-gradient(135deg, rgba(77,163,255,0.20), rgba(124,92,255,0.10))",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
              }}
            >
              <Bike size={22} color={THEME.accent} />
            </div>
            <div style={{ minWidth: 0 }}>
              <div style={{ fontSize: 16, fontWeight: 1000, letterSpacing: 0.2 }}>DashPilot</div>
              <div style={{ fontSize: 12, color: THEME.muted, marginTop: 3 }}>
                Local CSV dashboard + agentic analyst
              </div>
            </div>
          </div>

          <div style={{ marginTop: 14, display: "grid", gap: 10 }}>
            <NavItem
              active={activeTab === "dashboard"}
              icon={<LayoutDashboard size={18} color={THEME.text} />}
              label="Dashboard"
              onClick={() => setActiveTab("dashboard")}
            />
            <NavItem
              active={activeTab === "analyst"}
              icon={<MessageSquare size={18} color={THEME.text} />}
              label="AI Analyst"
              onClick={() => setActiveTab("analyst")}
            />
          </div>

          <div style={{ marginTop: 14 }}>
            <div
              style={{
                border: `1px solid ${THEME.border}`,
                borderRadius: 18,
                padding: 14,
                background: THEME.panel,
              }}
            >
              <div style={{ fontSize: 12, color: THEME.muted, fontWeight: 900, textTransform: "uppercase" }}>
                Status
              </div>

              <div style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 10 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <div
                    style={{
                      width: 10,
                      height: 10,
                      borderRadius: 999,
                      background: local ? THEME.good : THEME.warn,
                      boxShadow: `0 0 0 4px rgba(46,229,157,0.10)`,
                    }}
                  />
                  <div style={{ fontSize: 13, color: THEME.text }}>
                    Dashboard data: <span style={{ color: THEME.muted }}>{local ? "Loaded" : "Loading"}</span>
                  </div>
                </div>

                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <div
                    style={{
                      width: 10,
                      height: 10,
                      borderRadius: 999,
                      background: supabase ? THEME.good : THEME.warn,
                      boxShadow: `0 0 0 4px rgba(46,229,157,0.10)`,
                    }}
                  />
                  <div style={{ fontSize: 13, color: THEME.text }}>
                    Supabase: <span style={{ color: THEME.muted }}>{supabase ? "Connected" : "Not configured"}</span>
                  </div>
                </div>

                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <div
                    style={{
                      width: 10,
                      height: 10,
                      borderRadius: 999,
                      background: canUseAI ? THEME.good : THEME.warn,
                      boxShadow: `0 0 0 4px rgba(77,163,255,0.10)`,
                    }}
                  />
                  <div style={{ fontSize: 13, color: THEME.text }}>
                    Gemini: <span style={{ color: THEME.muted }}>{canUseAI ? "Enabled" : "Missing key"}</span>
                  </div>
                </div>
              </div>

              <div style={{ marginTop: 12, display: "flex", gap: 10, flexWrap: "wrap" }}>
                <IconButton
                  title={canUseAI ? "Update Gemini key" : "Add Gemini key"}
                  tone="accent"
                  onClick={() => setShowKeyModal(true)}
                >
                  <Sparkles size={16} />
                  {canUseAI ? "Update Key" : "Add Key"}
                </IconButton>

                <IconButton
                  title="Show widget catalog in chat"
                  onClick={() => {
                    setActiveTab("analyst");
                    pushWidgetMenuMessage();
                  }}
                >
                  <PlusCircle size={16} />
                  Catalog
                </IconButton>
              </div>

              <div style={{ marginTop: 10, color: THEME.muted, fontSize: 12, lineHeight: 1.3 }}>
                CSV source: <code>{CSV_URL}</code>
              </div>
            </div>
          </div>
        </aside>

        {/* MAIN */}
        <main style={{ flex: 1, overflow: "hidden" }}>
          {/* TOP BAR */}
          <div
            style={{
              height: 64,
              borderBottom: `1px solid ${THEME.border}`,
              background: "rgba(0,0,0,0.14)",
              backdropFilter: "blur(16px)",
              display: "flex",
              alignItems: "center",
              padding: "0 18px",
              gap: 12,
            }}
          >
            <div style={{ fontWeight: 1000, letterSpacing: 0.2 }}>
              {activeTab === "dashboard" ? "Dashboard" : "AI Analyst"}
            </div>

            <div style={{ marginLeft: "auto", display: "flex", gap: 10, flexWrap: "wrap" }}>
              {activeTab === "analyst" ? (
                <IconButton title="Show widget menu" tone="accent" onClick={() => pushWidgetMenuMessage()}>
                  <Sparkles size={16} />
                  Widget Menu
                </IconButton>
              ) : (
                <IconButton title="Go to AI Analyst" tone="accent" onClick={() => setActiveTab("analyst")}>
                  <MessageSquare size={16} />
                  Open Analyst
                </IconButton>
              )}

              <IconButton title="Clear pinned widgets" tone="danger" onClick={() => setWidgets([])}>
                <Trash2 size={16} />
                Clear Pins
              </IconButton>
            </div>
          </div>

          {/* CONTENT */}
          <div style={{ height: "calc(100vh - 64px)", overflow: "auto", padding: 18 }}>
            {/* ERROR */}
            {error ? (
              <div
                style={{
                  maxWidth: 980,
                  margin: "0 auto",
                  padding: 16,
                  borderRadius: 18,
                  border: `1px solid ${THEME.border}`,
                  background: THEME.card,
                  display: "flex",
                  gap: 12,
                  alignItems: "flex-start",
                }}
              >
                <AlertCircle size={18} color={THEME.bad} style={{ marginTop: 2 }} />
                <div>
                  <div style={{ fontWeight: 1000, marginBottom: 6 }}>Error</div>
                  <div style={{ color: THEME.muted }}>{error}</div>
                  <div style={{ marginTop: 10, color: THEME.muted, fontSize: 13 }}>
                    Ensure:
                    <ul style={{ margin: "8px 0 0 18px" }}>
                      <li>
                        <code>public/trips_rows.csv</code> exists (and is deployed)
                      </li>
                      <li>Gemini key is set (optional for dashboard; required for AI Analyst actions)</li>
                      <li>Supabase env keys exist if you want DB queries</li>
                    </ul>
                  </div>
                </div>
              </div>
            ) : loading ? (
              <div style={{ maxWidth: 980, margin: "0 auto", color: THEME.muted }}>Loading…</div>
            ) : activeTab === "dashboard" ? (
              // -------------------- DASHBOARD --------------------
              <div style={{ maxWidth: 1200, margin: "0 auto" }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-end" }}>
                  <div>
                    <div style={{ fontSize: 26, fontWeight: 1000, letterSpacing: 0.2 }}>Local Overview</div>
                    <div style={{ color: THEME.muted, marginTop: 6, fontSize: 13 }}>
                      Baseline charts are fixed. Add NEW widgets via AI Analyst → “show widget menu”.
                    </div>
                  </div>

                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                    <IconButton
                      title="Open Analyst and show menu"
                      tone="accent"
                      onClick={() => {
                        setActiveTab("analyst");
                        pushWidgetMenuMessage();
                      }}
                    >
                      <Sparkles size={16} />
                      Add New Widgets
                    </IconButton>
                  </div>
                </div>

                {/* STATS */}
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))",
                    gap: 14,
                    marginTop: 16,
                  }}
                >
                  <StatCard
                    title="Valid trips"
                    value={Number(local?.cleanCount || 0).toLocaleString()}
                    meta="Filtered to valid timestamp + duration ≤ 240 min"
                    icon={<BarChart3 size={18} color={THEME.accent} />}
                  />
                  <StatCard
                    title="Member ratio"
                    value={`${(local?.memberRatio || 0).toFixed(1)}%`}
                    meta="From local CSV"
                    icon={<Sparkles size={18} color={THEME.good} />}
                  />
                  <StatCard
                    title="Avg duration"
                    value={`${(local?.avgDurationMin || 0).toFixed(2)} min`}
                    meta={`Peak hour: ${local?.peakHour}`}
                    icon={<TrendingUp size={18} color={THEME.accent2} />}
                  />
                  <StatCard
                    title="Busiest day"
                    value={local?.busiestDay || "—"}
                    meta={`Top station: ${shortText(local?.topStation || "—", 34)}`}
                    icon={<Calendar size={18} color={THEME.warn} />}
                  />
                </div>

                {/* BASE CHARTS */}
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(auto-fit, minmax(420px, 1fr))",
                    gap: 14,
                    marginTop: 14,
                  }}
                >
                  {(baseCharts || []).map((c, i) => (
                    <Panel key={i} title={c.title} right={<span style={{ color: THEME.muted, fontSize: 12 }}>Source: Local CSV</span>}>
                      <ChartRenderer config={c.payload} />
                    </Panel>
                  ))}
                </div>

                {/* PINNED WIDGETS */}
                <div style={{ marginTop: 18 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
                    <div style={{ fontSize: 18, fontWeight: 1000, display: "flex", alignItems: "center", gap: 10 }}>
                      <Pin size={18} color={THEME.accent} />
                      Pinned Widgets
                    </div>
                    <div style={{ color: THEME.muted, fontSize: 12 }}>
                      {widgets.length ? `${widgets.length} pinned` : "No pinned widgets yet"}
                    </div>
                  </div>

                  {widgets.length ? (
                    <div
                      style={{
                        display: "grid",
                        gridTemplateColumns: "repeat(auto-fit, minmax(420px, 1fr))",
                        gap: 14,
                        marginTop: 12,
                      }}
                    >
                      {widgets.map((w) => (
                        <Panel
                          key={w.id}
                          title={w.title}
                          right={
                            <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                              <span style={{ color: THEME.muted, fontSize: 12 }}>
                                {w.source === "local" ? "Local" : w.source === "supabase" ? "Supabase" : "Custom"}
                              </span>
                              <IconButton title="Remove widget" tone="danger" onClick={() => removeWidget(w.id)}>
                                <X size={16} />
                              </IconButton>
                            </div>
                          }
                        >
                          {w.kind === "chart" ? <ChartRenderer config={w.payload} /> : <TableRenderer columns={w.payload?.columns} rows={w.payload?.rows} />}
                        </Panel>
                      ))}
                    </div>
                  ) : (
                    <div
                      style={{
                        marginTop: 12,
                        padding: 16,
                        borderRadius: 18,
                        border: `1px dashed ${THEME.borderStrong}`,
                        color: THEME.muted,
                        background: "rgba(255,255,255,0.02)",
                      }}
                    >
                      Go to <b>AI Analyst</b> → type <b>“show widget menu”</b> → click <b>Add</b>.
                    </div>
                  )}
                </div>
              </div>
            ) : (
              // -------------------- AI ANALYST --------------------
              <div
                style={{
                  maxWidth: 980,
                  margin: "0 auto",
                  height: "calc(100vh - 64px - 36px)",
                  display: "flex",
                  flexDirection: "column",
                  gap: 12,
                }}
              >
                {/* QUICK COMMANDS */}
                <div
                  style={{
                    borderRadius: 18,
                    border: `1px solid ${THEME.border}`,
                    background: THEME.card,
                    padding: 14,
                    boxShadow: softShadow,
                    backdropFilter: "blur(14px)",
                  }}
                >
                  <div style={{ display: "flex", alignItems: "center", gap: 10, justifyContent: "space-between" }}>
                    <div style={{ fontWeight: 1000, display: "flex", alignItems: "center", gap: 10 }}>
                      <Sparkles size={18} color={THEME.accent} />
                      Quick actions
                    </div>

                    <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                      <IconButton title="Show widget catalog" tone="accent" onClick={() => pushWidgetMenuMessage()}>
                        <PlusCircle size={16} />
                        Widget Menu
                      </IconButton>

                      <IconButton title="Set Gemini key" tone="accent" onClick={() => setShowKeyModal(true)}>
                        <Sparkles size={16} />
                        {canUseAI ? "Update Key" : "Add Key"}
                      </IconButton>

                      <IconButton title="Example: latest trips" onClick={() => setInput("Show latest 10 trips")}>
                        <Database size={16} />
                        Latest Trips
                      </IconButton>

                      <IconButton
                        title="Example: station search"
                        onClick={() => setInput("Find trips where start station contains Grove")}
                      >
                        <Table2 size={16} />
                        Search
                      </IconButton>
                    </div>
                  </div>

                  <div style={{ marginTop: 10, color: THEME.muted, fontSize: 13, lineHeight: 1.35 }}>
                    This chat can:
                    <ul style={{ margin: "8px 0 0 18px" }}>
                      <li>
                        Query <b>Supabase</b> when configured, otherwise fallback to <b>Local CSV</b>.
                      </li>
                      <li>
                        Add/preview <b>Dashboard widgets</b>: “show widget menu”.
                      </li>
                    </ul>
                    {!canUseAI ? (
                      <div style={{ marginTop: 8, color: THEME.warn }}>
                        Gemini key missing: AI actions disabled until you add a key.
                      </div>
                    ) : null}
                  </div>
                </div>

                {/* CHAT */}
                <div style={{ flex: 1, overflowY: "auto", paddingRight: 6, paddingBottom: 6 }}>
                  {messages.map((m, i) => renderChatMessage(m, i))}
                  <div ref={chatEndRef} />
                </div>

                {/* INPUT */}
                <div
                  style={{
                    borderRadius: 18,
                    border: `1px solid ${THEME.border}`,
                    background: THEME.card,
                    padding: 12,
                    boxShadow: softShadow,
                    backdropFilter: "blur(14px)",
                  }}
                >
                  <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                    <input
                      value={input}
                      onChange={(e) => setInput(e.target.value)}
                      onKeyDown={(e) => e.key === "Enter" && handleSend()}
                      placeholder='Try: "show widget menu" or "Show latest 10 trips"'
                      style={{
                        flex: 1,
                        borderRadius: 16,
                        border: `1px solid ${THEME.border}`,
                        background: "rgba(0,0,0,0.20)",
                        color: THEME.text,
                        padding: "12px 14px",
                        outline: "none",
                        fontSize: 14,
                      }}
                    />

                    <IconButton title="Send" tone="accent" onClick={handleSend}>
                      <Send size={16} />
                      Send
                    </IconButton>
                  </div>

                  {!supabase ? (
                    <div style={{ marginTop: 10, color: THEME.warn, fontSize: 12 }}>
                      Supabase not configured: DB queries will fallback to local CSV.
                    </div>
                  ) : null}
                </div>
              </div>
            )}
          </div>
        </main>
      </div>
    </>
  );
}
