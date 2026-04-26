import csv
import io
import os
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, Query, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import database as db
import ai

app = FastAPI(title="AI Ops Lab")
templates = Jinja2Templates(directory="templates")

if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

db.init_db()


# ── Date-range helpers ───────────────────────────────────────────────────────────

def _resolve_dates(start: str, end: str) -> tuple[str, str]:
    """Fill empty start/end with the actual data bounds."""
    bounds = db.get_date_bounds()
    return (start or bounds["start"]), (end or bounds["end"])


def _build_presets(data_start: str, data_end: str) -> list[dict]:
    """Return preset date-range pills relative to the data's max date."""
    try:
        hi = datetime.strptime(data_end, "%Y-%m-%d")
        lo = datetime.strptime(data_start, "%Y-%m-%d")
    except ValueError:
        return []

    def fmt(d: datetime) -> str:
        return d.strftime("%Y-%m-%d")

    # Week boundaries (Mon–Sun)
    week_start = hi - timedelta(days=hi.weekday())
    week_end   = week_start + timedelta(days=6)

    # Month boundaries of the most recent data month
    month_start = hi.replace(day=1)
    if hi.month == 12:
        month_end = hi.replace(year=hi.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        month_end = hi.replace(month=hi.month + 1, day=1) - timedelta(days=1)

    # Previous calendar month
    prev_month_end   = month_start - timedelta(days=1)
    prev_month_start = prev_month_end.replace(day=1)

    return [
        {"label": "All Data",     "start": data_start,              "end": data_end},
        {"label": "This Week",    "start": fmt(week_start),          "end": fmt(min(week_end, hi))},
        {"label": "This Month",   "start": fmt(month_start),         "end": fmt(min(month_end, hi))},
        {"label": "Last Month",   "start": fmt(prev_month_start),    "end": fmt(prev_month_end)},
        {"label": "Last 4 Weeks", "start": fmt(hi - timedelta(27)),  "end": data_end},
        {"label": "Last 8 Weeks", "start": fmt(hi - timedelta(55)),  "end": data_end},
    ]


def _date_label(start: str, end: str, active_preset: str) -> str:
    """Human-readable range label for display in KPIs."""
    try:
        s = datetime.strptime(start, "%Y-%m-%d")
        e = datetime.strptime(end,   "%Y-%m-%d")
        weeks = max(1, round((e - s).days / 7))
        s_fmt = s.strftime("%b %-d, %Y")
        e_fmt = e.strftime("%b %-d, %Y")
        return f"{s_fmt} – {e_fmt} · ~{weeks}w"
    except ValueError:
        return f"{start} – {end}"


# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_upload(content: bytes) -> list[dict]:
    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader]


def _require_data(request: Request):
    if not db.is_data_loaded():
        return RedirectResponse("/", status_code=302)
    return None


# ── onboarding ────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def upload_page(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request})


@app.post("/load-demo")
async def load_demo():
    db.seed_mock_data()
    return RedirectResponse("/dashboard", status_code=302)


@app.post("/upload")
async def upload_csvs(
    employees: UploadFile = File(None),
    projects: UploadFile = File(None),
    schedule: UploadFile = File(None),
):
    files: dict[str, list[dict]] = {}
    mapping = {
        "employees": employees,
        "projects": projects,
        "schedule": schedule,
    }
    for key, upload in mapping.items():
        if upload and upload.filename:
            content = await upload.read()
            files[key] = _parse_upload(content)

    if not files:
        db.seed_mock_data()
    else:
        db.seed_from_uploads(files)

    return RedirectResponse("/dashboard", status_code=302)


@app.get("/reset")
async def reset():
    conn = db.get_conn()
    conn.execute("UPDATE session SET loaded=0 WHERE id=1")
    for t in ["employees", "projects", "schedule"]:
        conn.execute(f"DELETE FROM {t}")
    conn.commit()
    conn.close()
    return RedirectResponse("/", status_code=302)


# ── dashboard ─────────────────────────────────────────────────────────────────

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    redirect = _require_data(request)
    if redirect:
        return redirect
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/api/dashboard-data", response_class=HTMLResponse)
async def dashboard_data(
    request: Request,
    start: str = Query(default=""),
    end:   str = Query(default=""),
):
    bounds          = db.get_date_bounds()
    start, end      = _resolve_dates(start, end)
    presets         = _build_presets(bounds["start"], bounds["end"])
    active_preset   = next((p["label"] for p in presets
                            if p["start"] == start and p["end"] == end), "Custom")
    period_label    = _date_label(start, end, active_preset)

    stats            = db.get_dashboard_stats(start, end)
    capacity_data    = db.get_capacity_data(start, end)
    projects         = db.get_projects_summary(start, end)
    rev_by_emp       = db.get_revenue_by_employee(start, end)
    rev_by_client    = db.get_revenue_by_client(start, end)
    rev_by_service   = db.get_revenue_by_service(start, end)
    weekly_trend     = db.get_weekly_revenue_trend(start, end)
    health           = db.compute_studio_health(stats, capacity_data, rev_by_service, rev_by_client)

    blended_rate   = round(stats["total_revenue"] / stats["total_hours_logged"]) \
                     if stats["total_hours_logged"] else 0
    avg_utilization = round(
        sum(e["utilization_pct"] for e in capacity_data) / len(capacity_data)
    ) if capacity_data else 0
    active_projects = [p for p in projects if p["actual_hours"] > 0]

    alert = ai.dashboard_alert(stats, capacity_data, projects, rev_by_client, rev_by_service)
    return templates.TemplateResponse("partials/dashboard_data.html", {
        "request":        request,
        "stats":          stats,
        "health":         health,
        "alert":          alert,
        "weekly_trend":   weekly_trend,
        "rev_by_emp":     rev_by_emp,
        "rev_by_client":  rev_by_client[:10],
        "rev_by_service": rev_by_service,
        "capacity_data":  capacity_data,
        "projects":       active_projects,
        "blended_rate":   blended_rate,
        "avg_utilization":avg_utilization,
        # date-range context
        "date_start":     start,
        "date_end":       end,
        "period_label":   period_label,
        "active_preset":  active_preset,
        "presets":        presets,
        "api_url":        "/api/dashboard-data",
        "content_id":     "dashboard-content",
    })


# ── capacity ──────────────────────────────────────────────────────────────────

@app.get("/capacity", response_class=HTMLResponse)
async def capacity_page(request: Request):
    redirect = _require_data(request)
    if redirect:
        return redirect
    return templates.TemplateResponse("capacity.html", {"request": request})


@app.get("/api/capacity-data", response_class=HTMLResponse)
async def capacity_data(
    request: Request,
    start: str = Query(default=""),
    end:   str = Query(default=""),
):
    bounds        = db.get_date_bounds()
    start, end    = _resolve_dates(start, end)
    presets       = _build_presets(bounds["start"], bounds["end"])
    active_preset = next((p["label"] for p in presets
                          if p["start"] == start and p["end"] == end), "Custom")
    period_label  = _date_label(start, end, active_preset)

    capacity_data = db.get_capacity_data(start, end)
    cap_trend     = db.get_weekly_capacity_pct(start, end)
    insight       = ai.capacity_insight(capacity_data)
    bench_staff   = [e for e in db.get_staff_week_availability() if e.get("is_bench")]
    return templates.TemplateResponse("partials/capacity_data.html", {
        "request":       request,
        "capacity_data": capacity_data,
        "cap_trend":     cap_trend,
        "insight":       insight,
        "bench_staff":   bench_staff,
        # date-range context
        "date_start":    start,
        "date_end":      end,
        "period_label":  period_label,
        "active_preset": active_preset,
        "presets":       presets,
        "api_url":       "/api/capacity-data",
        "content_id":    "capacity-content",
    })


# ── chat ──────────────────────────────────────────────────────────────────────

@app.get("/chat", response_class=HTMLResponse)
async def chat_page(request: Request):
    redirect = _require_data(request)
    if redirect:
        return redirect
    return templates.TemplateResponse("chat.html", {"request": request})


@app.post("/api/chat", response_class=HTMLResponse)
async def chat(request: Request):
    form = await request.form()
    question = form.get("question", "").strip()
    if not question:
        return HTMLResponse("<p class='text-gray-400 italic'>Please type a question.</p>")
    employees         = db.fetch_all("employees")
    project_health    = db.get_project_health()
    staff_availability = db.get_staff_week_availability()
    answer = ai.chat_response(question, employees, project_health, staff_availability)
    return templates.TemplateResponse("partials/chat_bubble.html", {
        "request": request,
        "question": question,
        "answer": answer,
    })


# ── smart actions ─────────────────────────────────────────────────────────────

@app.get("/actions", response_class=HTMLResponse)
async def actions_page(request: Request):
    redirect = _require_data(request)
    if redirect:
        return redirect
    return templates.TemplateResponse("actions.html", {"request": request})


@app.post("/api/action/briefing", response_class=HTMLResponse)
async def action_briefing(request: Request):
    stats = db.get_dashboard_stats()
    capacity_data = db.get_capacity_data()
    projects = db.get_projects_summary()
    draft = ai.weekly_briefing(stats, capacity_data, projects)
    return templates.TemplateResponse("partials/action_result.html", {
        "request": request,
        "label": "Weekly Briefing",
        "draft": draft,
    })


@app.post("/api/action/capacity-report", response_class=HTMLResponse)
async def action_capacity_report(request: Request):
    capacity_data = db.get_capacity_data()
    draft = ai.capacity_violation_report(capacity_data)
    return templates.TemplateResponse("partials/action_result.html", {
        "request": request,
        "label": "Capacity Violation Report",
        "draft": draft,
    })


@app.post("/api/action/project-risk", response_class=HTMLResponse)
async def action_project_risk(request: Request):
    """Use pre-computed health + availability to give a concrete rebalancing recommendation."""
    project_health     = db.get_project_health()
    staff_availability = db.get_staff_week_availability()
    draft = ai.project_risk_analysis(project_health, staff_availability)
    return templates.TemplateResponse("partials/action_result.html", {
        "request":  request,
        "label":    "Project Risk & Staff Rebalancing",
        "draft":    draft,
    })


@app.post("/api/action/budget-overrun", response_class=HTMLResponse)
async def action_budget_overrun(request: Request):
    """Draft client emails for every project that has blown its hours budget."""
    project_health = db.get_project_health()
    over_projects  = [p for p in project_health if p["risk"] == "OVER"]

    if not over_projects:
        return templates.TemplateResponse("partials/action_result.html", {
            "request": request,
            "label":   "Budget Overrun Emails",
            "draft":   "✅ Great news — no projects are currently over budget! All tracked projects are within their hours allocation.",
        })

    # Generate one email per overrun project, join with dividers
    emails = []
    for proj in over_projects:
        emails.append(f"### {proj['name']}\n" + ai.budget_overrun_email(proj))
    draft = "\n\n---\n\n".join(emails)

    return templates.TemplateResponse("partials/action_result.html", {
        "request": request,
        "label":   f"Budget Overrun Emails ({len(over_projects)} project{'s' if len(over_projects)>1 else ''})",
        "draft":   draft,
    })
