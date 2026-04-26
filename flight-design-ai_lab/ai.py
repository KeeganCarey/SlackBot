import os
import json
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

# Best models first; Gemma models are separate quota and act as reliable fallbacks
MODELS_TO_TRY = [
    "models/gemini-2.5-flash",
    "models/gemini-2.0-flash",
    "models/gemini-2.0-flash-001",
    "models/gemma-3-12b-it",   # separate quota pool — usually available
    "models/gemma-3-4b-it",
    "models/gemma-3-1b-it",
]

_QUOTA_MSG = (
    "AI insights are temporarily paused — the Gemini free-tier quota is exhausted for today. "
    "Everything else on the dashboard is fully live. "
    "Get a fresh key at aistudio.google.com (free, takes 30 seconds)."
)


def _configure():
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY", ""))


def ask(prompt: str, fallback: str = "") -> str:
    """Try each model in order. Returns fallback text (or quota message) on total failure."""
    if not os.environ.get("GEMINI_API_KEY"):
        return fallback or "⚠️ Add GEMINI_API_KEY to your .env file and restart."
    _configure()
    last_err = ""
    for name in MODELS_TO_TRY:
        try:
            resp = genai.GenerativeModel(name).generate_content(prompt)
            return resp.text.strip()
        except Exception as exc:
            last_err = str(exc)
            # 404 = model not on this key → try next
            # 429 = quota → try next (Gemma has its own bucket)
            # anything else → try next too
            continue
    return fallback or _QUOTA_MSG


# ── Dashboard ─────────────────────────────────────────────────────────────────

def dashboard_alert(stats: dict, capacity_data: list[dict], projects: list[dict],
                    rev_by_client: list[dict] | None = None,
                    rev_by_service: list[dict] | None = None) -> str:
    over_cap    = [e for e in capacity_data if e["over_capacity"]]
    over_budget = [p for p in projects if p["actual_hours"] > p["hours_budget"] and p["hours_budget"] > 0]
    blended     = round(stats["total_revenue"] / stats["total_hours_logged"]) if stats.get("total_hours_logged") else 0
    top_client  = rev_by_client[0] if rev_by_client else {}
    top_service = rev_by_service[0] if rev_by_service else {}

    # Data-driven fallback — works even with no AI
    worst = max(over_cap, key=lambda e: e["violation_weeks"]) if over_cap else None
    if worst:
        fallback = (
            f"{worst['name']} has exceeded their {worst['allowed_hours_week']}h/wk contracted limit "
            f"in {worst['violation_weeks']} of {worst['total_weeks']} weeks — the most pressing capacity issue right now. "
            f"With {len(over_cap)} staff running hot and Website Design at "
            f"{top_service.get('pct', '?')}% of revenue, consider reviewing project load before taking on new work."
        )
    else:
        fallback = (
            f"Studio is running well — ${stats['total_revenue']:,.0f} billed across "
            f"{stats['total_projects']} projects at a ${blended}/hr blended rate. "
            f"No capacity violations this period. Focus on diversifying beyond "
            f"{top_service.get('service', 'Website Design')} ({top_service.get('pct', '?')}% of revenue)."
        )

    prompt = f"""
You are a sharp business advisor for Flight Design, a brand and graphic design studio owned by Ariana Wolf in Oakland, CA.

STUDIO SNAPSHOT (13-week period):
- Total billed: ${stats['total_revenue']:,.0f}
- Blended rate: ${blended}/hr across {stats['total_employees']} staff
- {stats['total_projects']} active projects, {stats['over_budget_projects']} over hours budget
- Top client: {top_client.get('client','—')} at {top_client.get('pct',0)}% of revenue
- Top service: {top_service.get('service','—')} at {top_service.get('pct',0)}% of revenue

CAPACITY ALERTS ({len(over_cap)} staff with violation weeks):
{chr(10).join(f"- {e['name']}: {e['violation_weeks']} of {e['total_weeks']} weeks over {e['allowed_hours_week']}h/wk contracted" for e in over_cap)}

OVER-BUDGET PROJECTS:
{chr(10).join(f"- {p['name']} ({p['client']}): {p['actual_hours']:.1f}h vs {p['hours_budget']:.0f}h budget" for p in over_budget) or 'None'}

Write a 2-sentence morning brief for Ariana. Lead with the single most urgent operational issue using real names and numbers. Second sentence: one quick win she can act on today. Warm, direct, no bullet points.
"""
    return ask(prompt, fallback=fallback)


# ── Capacity ──────────────────────────────────────────────────────────────────

def capacity_insight(capacity_data: list[dict]) -> str:
    over    = [e for e in capacity_data if e["over_capacity"]]
    worst   = max(over, key=lambda e: e["violation_weeks"]) if over else None
    total_v = sum(e["violation_weeks"] for e in over)

    if worst:
        fallback = (
            f"{len(over)} of {len(capacity_data)} staff exceeded their contracted hours, "
            f"with {worst['name']} the most impacted at {worst['violation_weeks']} violation weeks "
            f"(avg {worst['avg_weekly_hours']}h vs {worst['allowed_hours_week']}h contracted). "
            f"That's {total_v} employee-weeks of uncompensated overwork — "
            f"time to review contracts or trim project load before burnout hits."
        )
    else:
        fallback = (
            f"All {len(capacity_data)} team members stayed within their contracted hours this period. "
            f"Good discipline — keep monitoring as new projects come in."
        )

    prompt = f"""
You are a business analyst for Flight Design, a brand design studio run by Ariana Wolf in Oakland, CA.

EMPLOYEE CAPACITY ANALYSIS (contracted vs actual average weekly hours):
{json.dumps(capacity_data, indent=2)}

Write 3–4 sentences analysing the team's capacity situation. Be specific:
1. Which employees are the most overextended and by how much
2. Whether subcontractors vs core staff show different patterns
3. One concrete action Ariana should take immediately

Be direct and practical. No bullet points — write as a paragraph.
"""
    return ask(prompt, fallback=fallback)


# ── Chat ─────────────────────────────────────────────────────────────

def chat_response(
    question: str,
    employees: list[dict],
    project_health: list[dict],        # from db.get_project_health()
    staff_availability: list[dict],    # from db.get_staff_week_availability()
) -> str:
    """Answer a free-form business question using rich, pre-computed context.

    Uses project_health (budget burn %, remaining hours, risk flag, this-week
    hours) and staff_availability (contracted vs scheduled this week, free hours)
    so the AI never has to guess or misread raw schedule rows.
    """
    # Compact representations to stay within token budget
    proj_ctx = [
        {
            "project":      p["name"],
            "client":       p["client"],
            "service":      p["service"],
            "budget_h":     p["hours_budget"],
            "actual_h":     p["actual_hours"],
            "remaining_h":  p["remaining_h"],
            "burn_pct":     p["budget_pct"],
            "risk":         p["risk"],          # OVER / AT_RISK / OK / T&M
            "this_week_h":  p["this_week_h"],
        }
        for p in project_health if p["hours_budget"] and p["hours_budget"] > 0
    ]

    staff_ctx = [
        {
            "name":          e["name"],
            "type":          e["employee_type"],
            "rate":          e["bill_rate"],
            "contracted_h":  e["contracted_h"],
            "scheduled_h":   e["scheduled_h"],
            "free_h":        e["free_h"],       # negative = already over
            "status":        "OVER" if e["overloaded"] else ("FREE" if e["has_capacity"] else "FULL"),
            "current_projects": e["current_projects"],
        }
        for e in staff_availability
    ]

    week_ref = staff_availability[0]["week_start"] if staff_availability else "current week"

    prompt = f"""
You are a smart business assistant for Flight Design, a brand design studio owned by Ariana Wolf in Oakland, CA.
Answer the question using ONLY the data provided. Be specific with names and numbers. If the data
doesn't have enough info, say so honestly. Keep answers under 150 words.

DATA WEEK REFERENCE: {week_ref}

PROJECT HEALTH (budget status + this-week hours):
  Fields: project, client, service, budget_h, actual_h, remaining_h, burn_pct, risk (OVER/AT_RISK/OK/T&M), this_week_h
{json.dumps(proj_ctx, indent=2)}

STAFF THIS WEEK (contracted vs scheduled, free hours available):
  Fields: name, type, rate, contracted_h, scheduled_h, free_h (negative=over), status (FREE/FULL/OVER), current_projects
{json.dumps(staff_ctx, indent=2)}

QUESTION: {question}
"""
    return ask(prompt, fallback=(
        "AI chat is temporarily paused (quota reached). "
        "All dashboard data is still live — check the Dashboard and Capacity tabs for real-time numbers."
    ))


# ── Smart Actions ─────────────────────────────────────────────────────────────

def project_risk_analysis(
    project_health: list[dict],
    staff_availability: list[dict],
) -> str:
    """Identify at-risk projects and recommend specific staff rebalancing."""
    # Separate bench staff from regular available staff
    bench      = [e for e in staff_availability if e.get("is_bench")]
    at_risk    = [p for p in project_health if p["risk"] in ("OVER", "AT_RISK")]
    available  = [e for e in staff_availability if not e.get("is_bench") and e["has_capacity"] and not e["overloaded"]]
    overloaded = [e for e in staff_availability if e["overloaded"]]

    week_ref = staff_availability[0]["week_start"] if staff_availability else "this week"

    # Deterministic fallback — no AI needed
    best_pick = (bench[0] if bench else available[0]) if (bench or available) else None

    if not at_risk:
        over_proj = [p for p in project_health if p["risk"] == "OVER"]
        if over_proj:
            p = over_proj[0]
            fallback = (
                f"{p['name']} has exceeded its hours budget by {abs(p['remaining_h']):.1f}h "
                f"({p['budget_pct']:.0f}% burned). "
                "Do NOT log more hours — pause logging and use Budget Overrun Emails to draft the client conversation."
            )
        else:
            bench_note = (f" {bench[0]['name']} and {bench[1]['name']} are fully available on the bench."
                         if len(bench) >= 2 else
                         f" {bench[0]['name']} is on the bench and fully available."
                         if bench else "")
            fallback = "No projects are currently at risk — all budgeted projects are under 80% burned." + bench_note
    else:
        p = at_risk[0]
        if p["risk"] == "OVER":
            fallback = (
                f"{p['name']} is OVER budget at {p['budget_pct']:.0f}% burn "
                f"({abs(p['remaining_h']):.1f}h over). "
                "Pause time logging immediately — adding more hours makes it worse. "
                "Draft a budget amendment email to the client first."
            )
        else:
            pick_note = (
                f"Recommend assigning {best_pick['name']} — "
                + ("they are on the bench with 40h fully available and zero current projects."
                   if best_pick.get("is_bench") else
                   f"{best_pick['free_h']:.1f}h free this week, {len(best_pick.get('current_projects',[]))} active projects.")
            ) if best_pick else "No staff currently available to reassign."
            fallback = (
                f"{p['name']} is AT_RISK at {p['budget_pct']:.0f}% burn "
                f"({p['remaining_h']:.1f}h remaining budget). {pick_note}"
            )

    # Pre-compute JSON blocks outside the f-string to avoid {{ }} escaping issues
    at_risk_json = json.dumps(
        [{"project": p["name"], "client": p["client"], "service": p["service"],
          "risk": p["risk"], "burn_pct": p["budget_pct"],
          "remaining_h": p["remaining_h"], "this_week_h": p["this_week_h"],
          "assigned_staff": p["assigned_staff"]} for p in at_risk],
        indent=2,
    ) if at_risk else "[None \u2014 all projects within budget]"

    bench_json = json.dumps(
        [{"name": e["name"], "bill_rate": e["bill_rate"],
          "free_h": e["free_h"], "projects_count": 0}   for e in bench],
        indent=2,
    ) if bench else "[None]"

    available_json = json.dumps(
        [{"name": e["name"], "type": e["employee_type"], "free_h": e["free_h"],
          "projects_count": len(e["current_projects"]),
          "current_projects": e["current_projects"]} for e in available],
        indent=2,
    ) if available else "[None \u2014 everyone is fully scheduled]"

    overloaded_json = json.dumps(
        [{"name": e["name"], "over_by_h": abs(e["free_h"]),
          "projects_count": len(e["current_projects"]),
          "current_projects": e["current_projects"]} for e in overloaded],
        indent=2,
    ) if overloaded else "[None]"

    prompt = f"""
You are a sharp business advisor for Flight Design (Ariana Wolf, Oakland CA).
Analyse the project risk and staff data below, then give a concrete, actionable recommendation.

—— RULES (follow these exactly) ——

For OVER projects (burn_pct > 100, budget already blown):
  • Do NOT suggest logging more hours or assigning anyone new — it just deepens the financial hole.
  • Tell Ariana to pause time logging on this project immediately.
  • Tell her to use the “Budget Overrun Client Emails” action (right below on this page) to draft
    a professional budget-amendment email to the client.

For AT_RISK projects (burn_pct 80–99, still has remaining hours):
  • ALWAYS check bench staff FIRST — they are Available Bandwidth with zero current projects
    and 40h/week capacity. They are the ideal pick: no context-switching, no burnout risk.
  • If bench staff exist, recommend them by name and mention they are fully available.
  • Only fall back to regular staff if no bench staff exist. Among regular staff, pick the
    person with the highest free_h and fewest current projects.
  • Never recommend someone on 5+ projects if a lighter option exists.
  • State: name, is_bench or not, free hours available, current project count.

For overloaded staff (free_h < 0):
  • Name them and say Ariana should pull work FROM them this week.

WEEK: {week_ref}

AT-RISK / OVER-BUDGET PROJECTS:
{at_risk_json}

AVAILABLE BANDWIDTH STAFF (bench — zero projects, fully free — ALWAYS PREFER THESE FIRST):
{bench_json}

REGULAR STAFF WITH SOME CAPACITY (sorted: most free hours first, then fewest projects):
{available_json}

OVERLOADED STAFF (free_h < 0 — pull work from these people):
{overloaded_json}

Write 3–4 punchy sentences. Real names and numbers. No bullet points in the answer.
"""
    return ask(prompt, fallback=fallback)


def budget_overrun_email(project: dict) -> str:
    """Draft a professional client communication for a budget-overrun project."""
    over_by   = abs(project.get("remaining_h", 0))
    burn_pct  = project.get("budget_pct", 0)
    est_extra = round(over_by * 175)   # $175/hr blended rate estimate

    fallback = (
        f"Subject: Budget Update — {project['name']}\n\n"
        f"Hi [Client name],\n\n"
        f"We want to proactively flag that the {project['service']} work for your project has "
        f"reached {burn_pct:.0f}% of the originally scoped {project['hours_budget']:.0f} hours. "
        f"We've logged {project['actual_hours']:.1f}h to date, and the remaining scope will require "
        f"approximately {over_by:.1f} additional hours (est. ${est_extra:,.0f} at our blended rate).\n\n"
        f"We'd love to schedule a quick call to discuss options: scope reduction, a budget amendment, "
        f"or phasing the remaining work into a follow-on engagement.\n\n"
        f"Looking forward to your thoughts,\nFlight Design"
    )

    prompt = f"""
You are drafting a professional, warm client email on behalf of Ariana Wolf at Flight Design,
a brand and graphic design studio in Oakland, CA.

PROJECT DETAILS:
- Project: {project["name"]}
- Client: {project["client"]}
- Service: {project["service"]}
- Hours budget: {project["hours_budget"]} h
- Hours logged: {project["actual_hours"]} h  ({burn_pct:.0f}% burned)
- Hours over budget: {over_by:.1f} h
- Estimated cost of overage at $175/hr blended rate: ${est_extra:,.0f}

Write a short, professional email (under 120 words, not counting subject line) that:
1. Opens warmly and gets straight to the point
2. States clearly that the project is approaching or has exceeded the scoped hours
3. Gives the exact numbers (hours used, overage, estimated cost)
4. Offers 3 options: scope reduction, budget amendment, or phase into a new engagement
5. Invites a quick call to align
6. Signs off as Ariana Wolf, Flight Design

Subject line first, then the email body. Professional but warm tone.
"""
    return ask(prompt, fallback=fallback)


# ── Smart Actions ─────────────────────────────────────────────────────────────

def weekly_briefing(stats: dict, capacity_data: list[dict], projects: list[dict]) -> str:
    over_cap = [e for e in capacity_data if e["over_capacity"]]
    over_budget = [p for p in projects if p["actual_hours"] > p["hours_budget"] and p["hours_budget"] > 0]
    prompt = f"""
You are a smart business assistant generating a weekly briefing for Ariana Wolf of Flight Design.

BUSINESS STATS: {json.dumps(stats, indent=2)}
OVER-CAPACITY EMPLOYEES: {json.dumps(over_cap, indent=2)}
OVER-BUDGET PROJECTS: {json.dumps(over_budget, indent=2)}
ALL PROJECTS SUMMARY: {json.dumps(projects[:20], indent=2)}

Generate a weekly briefing with these bold sections:
1. **Top 3 Priorities** — specific and actionable for this week
2. **Capacity Alerts** — which team members are overextended and by how much
3. **Budget Watch** — which projects are over hours budget
4. **Revenue Snapshot** — total billed, avg per project, quick win to improve cash flow

Use bold headers. Be specific with names and numbers. Keep each section to 1–2 sentences.
"""
    return ask(prompt, fallback=(
        f"**Weekly Briefing**\n\n"
        f"**Top Priority:** {len([e for e in capacity_data if e['over_capacity']])} staff exceeded capacity — review workload distribution.\n\n"
        f"**Revenue:** ${stats.get('total_revenue',0):,.0f} billed · ${round(stats.get('total_revenue',0)/max(stats.get('total_projects',1),1)):,.0f} per project avg.\n\n"
        f"**Budget Watch:** {stats.get('over_budget_projects',0)} project(s) over hours budget."
    ))


def capacity_violation_report(capacity_data: list[dict]) -> str:
    over_cap = [e for e in capacity_data if e["over_capacity"]]
    prompt = f"""
You are a business operations analyst for Flight Design, a brand design studio owned by Ariana Wolf.

EMPLOYEES EXCEEDING CONTRACTED CAPACITY:
{json.dumps(over_cap, indent=2)}

Write a concise capacity violation report that:
1. Names each overextended employee with their contracted hours/week vs actual average
2. Flags the top 2–3 most severe cases
3. Recommends whether Ariana should renegotiate contracts, hire more staff, or reduce project load

Format as a professional memo. Use bold for employee names. Keep it under 150 words.
"""
    return ask(prompt, fallback=(
        f"{len(over_cap)} employee(s) exceeded contracted hours. "
        f"Most severe: {max(over_cap, key=lambda e: e['violation_weeks'])['name'] if over_cap else 'n/a'}. "
        f"Consider reducing project intake or renegotiating contracts."
    ))


def project_budget_report(projects: list[dict]) -> str:
    over_budget = [p for p in projects if p["actual_hours"] > p["hours_budget"] and p["hours_budget"] > 0]
    on_track = [p for p in projects if 0 < p["actual_hours"] <= p["hours_budget"]]
    prompt = f"""
You are a project manager analyst for Flight Design, a brand design studio owned by Ariana Wolf in Oakland, CA.

OVER-BUDGET PROJECTS (hours logged exceed budgeted hours):
{json.dumps(over_budget, indent=2)}

ON-TRACK PROJECTS (sample):
{json.dumps(on_track[:10], indent=2)}

Write a concise project budget status report:
1. List each over-budget project with client name, hours over budget, and estimated cost impact at $175/hr avg
2. Highlight which client relationship is most at risk
3. Give one immediate recommendation

Format professionally with bold project names. Under 150 words.
"""
    return ask(prompt, fallback=(
        f"{len(over_budget)} project(s) over hours budget. "
        f"Review logged hours against scope — flag to clients before invoicing."
    ))
