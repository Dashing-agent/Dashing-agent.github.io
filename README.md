# DashPilot AI Agent — React + Vite (Local CSV Dashboard) + Supabase  + Gemini

A React + Vite dashboard that loads a local CSV in the browser, builds aggregates/charts, and provides an “AI Analyst” chat that can recommend/preview/pin widgets. Supabase querying is optional (enabled only if you set Supabase env vars).

## Architecture

<p align="center">
  <img src="architecture.png?v=1" alt="Architecture Diagram" width="900" />
</p>

> If GitHub isn’t updating the image, change `?v=1` to `?v=2` (cache-bust), and hard refresh your browser.

---

## What this app expects

### 1) Local CSV (required)
The app loads:
- `public/trips_rows.csv`

If it’s missing, you’ll see an error like:  
`CSV file not found: put trips_rows.csv in /public`

### 2) Gemini API key (required)
This app requires `VITE_GEMINI_API_KEY` to be set. If missing, the app will show:
- `Missing VITE_GEMINI_API_KEY`

### 3) Supabase (required)
Supabase is only used if you set:
- `VITE_SUPABASE_URL`
- `VITE_SUPABASE_ANON_KEY`

If you try to use DB features without these, you’ll see:
- `Supabase not configured (missing VITE_SUPABASE_URL or VITE_SUPABASE_ANON_KEY).`

---

## Prerequisites

- Node.js 18+ (recommended: latest LTS)
- npm (or pnpm/yarn)

---

## Setup (Local Development)

### 1) Install dependencies
```bash
npm install
