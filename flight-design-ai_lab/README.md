# 🧪 AI Ops Lab — Flight Design

> AI-powered studio operations for freelance design studios.
> Project risk · capacity management · client communications · smart team rebalancing.

---

## What It Does

AI Ops Lab turns three simple CSVs (employees, projects, schedule) into a live
intelligence layer for a design studio — surfacing budget risk, team burnout, and
client-ready communications without any manual analysis.

| Screen | What It Does |
|---|---|
| 🏠 **Dashboard** | KPI strip · weekly revenue trend · top clients · service mix · full project portfolio. Filter by any date range with preset pills or a custom date picker. |
| 📊 **Capacity** | Contracted hours vs actual per person per week. Available Bandwidth bench staff highlighted. Overloaded staff flagged in red. |
| ⚡ **Smart Actions** | AI analyses project risk, recommends who to assign (bench-first, burnout-aware), and drafts ready-to-send budget overrun emails for clients. |
| 💬 **Ask Your Business** | Plain-English chat over your real studio data — capacity, budgets, project health, team utilisation. |

---

## Smart Actions in Detail

### 🔄 Project Risk & Staff Rebalancing
Reads every project's budget burn and every team member's free hours this week,
then applies clear business rules:

| Risk Level | AI Action |
|---|---|
| `OVER` (>100% burned) | Pause logging · draft client budget amendment · do NOT add more staff |
| `AT_RISK` (80–99% burned) | Assign the least-busy available person — bench staff first, then fewest active projects |
| Overloaded staff (`free_h < 0`) | Pull work FROM them — never add more |

### 📧 Budget Overrun Client Emails
For every project over its hours budget, drafts a warm, professional email with:
- Exact hours used and overage
- Estimated cost at blended rate
- Three resolution options (scope reduction / budget amendment / phase into new engagement)
- Signed off as the studio owner

---

## Available Bandwidth (Bench Staff)
Associates marked as `Available Bandwidth` in the employee CSV have:
- 40h/week full capacity, no current projects
- Pulsing green banner on the Capacity page
- Always surfaced **first** in rebalancing recommendations (zero context-switching)

---

## Quick Start

### 1. Clone & install

```bash
git clone https://github.com/Avinash07-git/flight-design-ai_lab.git
cd flight-design-ai_lab
python3 -m venv .venv && source .venv/bin/activate
pip install fastapi "uvicorn[standard]" jinja2 python-multipart google-generativeai python-dotenv
```

### 2. Add your Gemini API key

```bash
cp .env.example .env
# edit .env and paste your key — free at https://aistudio.google.com/apikey
```

### 3. Run

```bash
uvicorn main:app --port 8765 --reload
```

Open **http://localhost:8765**

### 4. Load data

- **"⚡ Use Demo Data"** — loads Ariana Wolf's mock studio data instantly (8 staff, 50+ projects, 13 weeks of schedule)
- **Upload your own CSVs** — see format below

---

## CSV Format

Upload three files on the onboarding screen:

### `employee_list.csv`
```
Name,Employee Type,Bill Rate,Capacity
Ariana Wolf,Core Staff,250,60%
Jordan Lee,Available Bandwidth,200,100%
```
`Employee Type` options: `Core Staff` · `Subcontractor` · `Available Bandwidth`
`Capacity` = % of a standard 40h week (e.g. `75%` = 30h contracted)

### `project.csv`
```
Project,Client,Service,Hours Budget,Budget USD,Start Date,End Date
Alameda Health System-Website Design,Alameda Health System,Website Design,230,40000,2026-04-27,2026-07-24
```

### `schedule.csv`
```
Employee Name,Project,Client,Start Date,Hours,Amount
Matt Meickle,Alameda Health System-Website Design,Alameda Health System,2026-05-01,6,1050
```

---

## AI Model

Uses **Google Gemini 2.5 Flash / 2.0 Flash** with automatic fallback cascade:
```
gemini-2.5-flash → gemini-2.0-flash → gemini-2.0-flash-001 → gemma-3-12b-it → gemma-3-4b-it
```
Gemma models are on a separate quota pool — the app stays live even when the
free Gemini tier is exhausted for the day.

---

## Tech Stack

| Layer | Tech |
|---|---|
| Backend | Python 3.11+ · FastAPI |
| Frontend | HTMX · Tailwind CSS · Chart.js |
| Database | SQLite (local, no server needed) |
| AI | Google Gemini 2.5/2.0 Flash · Gemma 3 fallback |
| Templating | Jinja2 |

---

## Project Structure

```
flight-design-ai_lab/
├── main.py                        # FastAPI app, all routes, date-range helpers
├── database.py                    # SQLite setup, CSV seeder, all queries
│                                  #   incl. get_project_health(), get_staff_week_availability()
├── ai.py                          # Gemini prompts for every AI feature
│                                  #   incl. project_risk_analysis(), budget_overrun_email()
├── .env.example                   # Copy to .env and add GEMINI_API_KEY
├── Data Files/
│   ├── employee_list.csv          # Staff, rates, capacity %
│   ├── project.csv                # Projects, clients, budgets
│   └── schedule.csv               # Weekly schedule rows
└── templates/
    ├── base.html                  # Sidebar nav, header, layout shell
    ├── dashboard.html             # Dashboard page shell
    ├── capacity.html              # Capacity page shell
    ├── chat.html                  # Chat page
    ├── actions.html               # Smart Actions page
    ├── upload.html                # Onboarding / data upload
    └── partials/
        ├── _filter_bar.html       # Reusable date-range filter bar
        ├── dashboard_data.html    # Dashboard HTMX partial
        ├── capacity_data.html     # Capacity HTMX partial
        ├── chat_bubble.html       # Chat message partial
        └── action_result.html     # Smart action result card
```

---

## Key Design Decisions

- **Pre-computed context** — the AI receives `get_project_health()` (burn %, risk flag, remaining hours) and `get_staff_week_availability()` (free hours this week, project count) rather than raw schedule rows. This eliminates hallucination.
- **OVER ≠ assign more staff** — an explicit rule prevents the AI from recommending more hours on already-blown budgets. It says "pause logging, talk to client" instead.
- **Bench-first assignment** — `Available Bandwidth` staff always surface first in recommendations. Tiebreaker for regular staff is fewest active projects (context-switching cost), not just free hours.
- **No external dependencies** — everything runs locally. Data only leaves the machine when calling the Gemini API.

---

## Notes

- The app is **read + draft only** — it does not send emails or modify any external tool
- No data is stored in the cloud — SQLite file lives on your machine
- Works for any freelance studio — just upload your own CSVs
