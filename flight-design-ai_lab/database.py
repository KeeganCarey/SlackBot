import sqlite3
import csv
import os
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = "flight_design.db"
DATA_DIR = Path(__file__).parent / "Data Files"


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_date(s: str) -> str:
    """Convert M/D/YY or M/D/YYYY → YYYY-MM-DD."""
    s = s.strip()
    for fmt in ["%m/%d/%y", "%m/%d/%Y"]:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s


def _parse_money(s: str) -> float:
    return float(s.replace("$", "").replace(",", "").strip() or 0)


def _week_key(date_str: str) -> str:
    """YYYY-MM-DD → ISO week label e.g. '2026-W17'."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y-W%W")
    except Exception:
        return ""


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            employee_type TEXT,
            bill_rate REAL,
            capacity_pct REAL
        );
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            client TEXT,
            service TEXT,
            hours_budget REAL,
            start_date TEXT,
            end_date TEXT,
            budget_usd REAL
        );
        CREATE TABLE IF NOT EXISTS schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_name TEXT,
            client TEXT,
            project TEXT,
            service TEXT,
            start_date TEXT,
            end_date TEXT,
            start_time TEXT,
            end_time TEXT,
            hours REAL,
            amount REAL
        );
        CREATE TABLE IF NOT EXISTS session (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            loaded INTEGER DEFAULT 0
        );
        INSERT OR IGNORE INTO session (id, loaded) VALUES (1, 0);
    """)
    conn.commit()
    conn.close()


def get_date_bounds() -> dict:
    """Min and max start_date in the schedule table."""
    conn = get_conn()
    row  = conn.execute(
        "SELECT MIN(start_date) as lo, MAX(start_date) as hi FROM schedule"
    ).fetchone()
    conn.close()
    return {"start": row["lo"] or "", "end": row["hi"] or ""}


def _date_where(start: str, end: str, table: str = "") -> tuple[str, list]:
    """Returns (SQL snippet, params) to safely filter on a date column.

    Args:
        start: lower bound (YYYY-MM-DD) or empty string
        end:   upper bound (YYYY-MM-DD) or empty string
        table: optional table alias to qualify the column (e.g. 's' → 's.start_date')

    Usage::
        extra, params = _date_where(start, end)
        conn.execute(f"SELECT ... FROM schedule WHERE 1=1 {extra}", params)
    """
    col = f"{table}.start_date" if table else "start_date"
    clauses: list[str] = []
    params:  list[str] = []
    if start:
        clauses.append(f"{col} >= ?")
        params.append(start)
    if end:
        clauses.append(f"{col} <= ?")
        params.append(end)
    sql = ("AND " + " AND ".join(clauses)) if clauses else ""
    return sql, params


def is_data_loaded() -> bool:
    conn = get_conn()
    row = conn.execute("SELECT loaded FROM session WHERE id=1").fetchone()
    conn.close()
    return bool(row and row["loaded"])


def clear_all_tables(conn):
    for t in ["employees", "projects", "schedule"]:
        conn.execute(f"DELETE FROM {t}")


# ── seeding ───────────────────────────────────────────────────────────────────

def _insert_employees(conn, rows: list[dict]):
    for row in rows:
        raw_cap = row.get("Capacity", "0").replace("%", "").strip()
        pct = float(raw_cap or 0) / 100
        conn.execute(
            "INSERT OR IGNORE INTO employees (name, employee_type, bill_rate, capacity_pct) VALUES (?,?,?,?)",
            (row.get("Name", "").strip(),
             row.get("Employee Type", "").strip(),
             float(row.get("Bill Rate", 0) or 0),
             pct),
        )


def _insert_projects(conn, rows: list[dict]):
    for row in rows:
        conn.execute(
            """INSERT OR IGNORE INTO projects
               (name, client, service, hours_budget, start_date, end_date, budget_usd)
               VALUES (?,?,?,?,?,?,?)""",
            (row.get("Project", "").strip(),
             row.get("Client", "").strip(),
             row.get("Service", "").strip(),
             float(row.get("Total Hours Budget", 0) or 0),
             _parse_date(row.get("Project Start Date", "")),
             _parse_date(row.get("Project End Date", "")),
             _parse_money(row.get("Budget", "0"))),
        )


def _insert_schedule(conn, rows: list[dict]):
    for row in rows:
        conn.execute(
            """INSERT INTO schedule
               (employee_name, client, project, service,
                start_date, end_date, start_time, end_time, hours, amount)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (row.get("Employee Name", "").strip(),
             row.get("Client", "").strip(),
             row.get("Project", "").strip(),
             row.get("Service", "").strip(),
             _parse_date(row.get("Start Date", "")),
             _parse_date(row.get("End Date", "")),
             row.get("Start Time", "").strip(),
             row.get("End Time", "").strip(),
             float(row.get("Number of hours", 0) or 0),
             float(row.get("Amount", 0) or 0)),
        )


def _read_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return [dict(r) for r in csv.DictReader(f)]


def seed_mock_data():
    conn = get_conn()
    clear_all_tables(conn)
    emp_path = DATA_DIR / "employee_list.csv"
    proj_path = DATA_DIR / "project.csv"
    sched_path = DATA_DIR / "schedule.csv"
    if emp_path.exists():
        _insert_employees(conn, _read_csv(emp_path))
    if proj_path.exists():
        _insert_projects(conn, _read_csv(proj_path))
    if sched_path.exists():
        _insert_schedule(conn, _read_csv(sched_path))
    conn.execute("UPDATE session SET loaded=1 WHERE id=1")
    conn.commit()
    conn.close()


def seed_from_uploads(files: dict[str, list[dict]]):
    conn = get_conn()
    clear_all_tables(conn)
    if "employees" in files:
        _insert_employees(conn, files["employees"])
    if "projects" in files:
        _insert_projects(conn, files["projects"])
    if "schedule" in files:
        _insert_schedule(conn, files["schedule"])
    conn.execute("UPDATE session SET loaded=1 WHERE id=1")
    conn.commit()
    conn.close()


# ── queries ───────────────────────────────────────────────────────────────────

def fetch_all(table: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _build_weekly_hours(schedule_rows) -> dict[str, dict[str, float]]:
    """Returns {employee_name: {week_key: total_hours}}."""
    weekly: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in schedule_rows:
        wk = _week_key(row["start_date"])
        if wk:
            weekly[row["employee_name"]][wk] += row["hours"]
    return weekly


def get_dashboard_stats(start: str = "", end: str = "") -> dict:
    extra, params = _date_where(start, end)
    extra_s, params_s = _date_where(start, end, table="s")
    conn = get_conn()
    total_revenue = conn.execute(
        f"SELECT COALESCE(SUM(amount),0) as t FROM schedule WHERE 1=1 {extra}", params
    ).fetchone()["t"]
    total_hours = conn.execute(
        f"SELECT COALESCE(SUM(hours),0) as t FROM schedule WHERE 1=1 {extra}", params
    ).fetchone()["t"]
    total_projects = conn.execute(
        f"SELECT COUNT(DISTINCT project) as n FROM schedule WHERE hours > 0 {extra}", params
    ).fetchone()["n"]
    total_employees = conn.execute("SELECT COUNT(*) as n FROM employees").fetchone()["n"]
    over_budget = conn.execute(f"""
        SELECT COUNT(*) as n FROM (
            SELECT p.name, p.hours_budget, COALESCE(SUM(s.hours),0) as actual
            FROM projects p
            LEFT JOIN schedule s ON s.project = p.name AND 1=1 {extra_s}
            GROUP BY p.name
            HAVING actual > p.hours_budget AND p.hours_budget > 0
        )
    """, params_s).fetchone()["n"]

    # Capacity violations — computed in Python
    sched_rows = conn.execute(
        f"SELECT employee_name, start_date, hours FROM schedule WHERE 1=1 {extra}", params
    ).fetchall()
    emp_rows = conn.execute("SELECT name, capacity_pct FROM employees").fetchall()
    conn.close()

    weekly = _build_weekly_hours(sched_rows)
    emp_cap = {r["name"]: r["capacity_pct"] * 40 for r in emp_rows}
    over_cap = sum(
        1 for emp, weeks in weekly.items()
        if any(h > emp_cap.get(emp, 40) for h in weeks.values())
    )

    return {
        "total_revenue": total_revenue,
        "total_hours_logged": round(total_hours, 1),
        "total_projects": total_projects,
        "total_employees": total_employees,
        "over_budget_projects": over_budget,
        "employees_over_capacity": over_cap,
    }


def get_capacity_data(start: str = "", end: str = "") -> list[dict]:
    """Per-employee capacity analysis vs actual schedule."""
    extra, params = _date_where(start, end)
    conn = get_conn()
    employees = conn.execute(
        "SELECT * FROM employees ORDER BY capacity_pct DESC"
    ).fetchall()
    sched_rows = conn.execute(
        f"SELECT employee_name, start_date, hours FROM schedule WHERE 1=1 {extra}", params
    ).fetchall()
    conn.close()

    weekly = _build_weekly_hours(sched_rows)
    result = []
    for emp in employees:
        allowed_h = round(emp["capacity_pct"] * 40, 1)
        emp_weeks = weekly.get(emp["name"], {})
        total_hours = sum(emp_weeks.values())
        num_weeks = len(emp_weeks) or 1
        avg_weekly = round(total_hours / num_weeks, 1)
        violation_weeks = sum(1 for h in emp_weeks.values() if h > allowed_h)
        utilization_pct = round((avg_weekly / allowed_h * 100) if allowed_h > 0 else 0)

        result.append({
            "name": emp["name"],
            "employee_type": emp["employee_type"],
            "bill_rate": emp["bill_rate"],
            "capacity_pct": int(emp["capacity_pct"] * 100),
            "allowed_hours_week": allowed_h,
            "avg_weekly_hours": avg_weekly,
            "total_hours": round(total_hours, 1),
            "violation_weeks": violation_weeks,
            "total_weeks": num_weeks,
            "utilization_pct": utilization_pct,
            "over_capacity": violation_weeks > 0,   # any week over = flagged
        })

    return result


def get_revenue_by_employee(start: str = "", end: str = "") -> list[dict]:
    """Revenue, hours, and % share per employee, sorted by revenue desc."""
    extra, params = _date_where(start, end)
    conn = get_conn()
    total_rev = conn.execute(
        f"SELECT COALESCE(SUM(amount),0) FROM schedule WHERE 1=1 {extra}", params
    ).fetchone()[0]
    rows = conn.execute(f"""
        SELECT employee_name AS name,
               COALESCE(SUM(amount),0) AS revenue,
               COALESCE(SUM(hours),0)  AS hours
        FROM schedule WHERE 1=1 {extra}
        GROUP BY employee_name
        ORDER BY revenue DESC
    """, params).fetchall()
    conn.close()
    result = []
    for r in rows:
        result.append({
            "name":    r["name"],
            "revenue": round(r["revenue"], 2),
            "hours":   round(r["hours"], 1),
            "pct":     round(r["revenue"] / total_rev * 100, 1) if total_rev else 0,
        })
    return result


def get_revenue_by_client(start: str = "", end: str = "") -> list[dict]:
    """Revenue per client with % share, sorted desc."""
    extra, params = _date_where(start, end)
    conn = get_conn()
    total_rev = conn.execute(
        f"SELECT COALESCE(SUM(amount),0) FROM schedule WHERE 1=1 {extra}", params
    ).fetchone()[0]
    rows = conn.execute(f"""
        SELECT client,
               COALESCE(SUM(amount),0)            AS revenue,
               COALESCE(SUM(hours),0)             AS hours,
               COUNT(DISTINCT project)            AS projects,
               COUNT(DISTINCT employee_name)      AS staff
        FROM schedule WHERE 1=1 {extra}
        GROUP BY client
        ORDER BY revenue DESC
    """, params).fetchall()
    conn.close()
    result = []
    for r in rows:
        result.append({
            "client":   r["client"],
            "revenue":  round(r["revenue"], 2),
            "hours":    round(r["hours"], 1),
            "projects": r["projects"],
            "staff":    r["staff"],
            "pct":      round(r["revenue"] / total_rev * 100, 1) if total_rev else 0,
        })
    return result


def get_revenue_by_service(start: str = "", end: str = "") -> list[dict]:
    """Revenue per service type with % share, sorted desc."""
    extra, params = _date_where(start, end)
    conn = get_conn()
    total_rev = conn.execute(
        f"SELECT COALESCE(SUM(amount),0) FROM schedule WHERE 1=1 {extra}", params
    ).fetchone()[0]
    rows = conn.execute(f"""
        SELECT service,
               COALESCE(SUM(amount),0) AS revenue,
               COALESCE(SUM(hours),0)  AS hours
        FROM schedule WHERE 1=1 {extra}
        GROUP BY service
        ORDER BY revenue DESC
    """, params).fetchall()
    conn.close()
    result = []
    for r in rows:
        result.append({
            "service": r["service"],
            "revenue": round(r["revenue"], 2),
            "hours":   round(r["hours"], 1),
            "pct":     round(r["revenue"] / total_rev * 100, 1) if total_rev else 0,
        })
    return result


def get_weekly_revenue_trend(start: str = "", end: str = "") -> dict:
    """Weekly billed totals for the trend line chart."""
    extra, params = _date_where(start, end)
    conn = get_conn()
    rows = conn.execute(
        f"SELECT start_date, amount FROM schedule WHERE 1=1 {extra}", params
    ).fetchall()
    conn.close()

    weekly: dict[str, float] = defaultdict(float)
    week_anchor: dict[str, datetime] = {}          # earliest date per week

    for row in rows:
        wk = _week_key(row["start_date"])
        if not wk:
            continue
        weekly[wk] += row["amount"]
        try:
            dt = datetime.strptime(row["start_date"], "%Y-%m-%d")
            if wk not in week_anchor or dt < week_anchor[wk]:
                week_anchor[wk] = dt
        except ValueError:
            pass

    sorted_weeks = sorted(weekly.keys())
    labels, values = [], []
    for wk in sorted_weeks:
        dt = week_anchor.get(wk)
        labels.append(dt.strftime("%b %-d") if dt else wk)
        values.append(round(weekly[wk]))

    avg = round(sum(values) / len(values)) if values else 0
    return {"labels": labels, "values": values, "avg": avg}


def get_weekly_capacity_pct(start: str = "", end: str = "") -> dict:
    """Per-employee % of weekly capacity used, for the violation trend chart."""
    extra, params = _date_where(start, end)
    conn = get_conn()
    sched = conn.execute(
        f"SELECT employee_name, start_date, hours FROM schedule WHERE 1=1 {extra}", params
    ).fetchall()
    emps = conn.execute(
        "SELECT name, capacity_pct FROM employees ORDER BY capacity_pct DESC"
    ).fetchall()
    conn.close()

    weekly = _build_weekly_hours(sched)
    all_weeks = sorted({_week_key(r["start_date"]) for r in sched if _week_key(r["start_date"])})

    # Map week key → readable label from earliest actual date
    week_anchor: dict[str, datetime] = {}
    for row in sched:
        wk = _week_key(row["start_date"])
        if not wk:
            continue
        try:
            dt = datetime.strptime(row["start_date"], "%Y-%m-%d")
            if wk not in week_anchor or dt < week_anchor[wk]:
                week_anchor[wk] = dt
        except ValueError:
            pass

    labels = [
        week_anchor[wk].strftime("%b %-d") if wk in week_anchor else wk
        for wk in all_weeks
    ]

    employees = []
    for emp in emps:
        cap_h = round(emp["capacity_pct"] * 40, 1)
        if cap_h <= 0:
            continue
        pct_per_week = [
            round(weekly.get(emp["name"], {}).get(wk, 0) / cap_h * 100, 1)
            for wk in all_weeks
        ]
        employees.append({"name": emp["name"], "cap_h": cap_h, "data": pct_per_week})

    return {"labels": labels, "employees": employees}


def compute_studio_health(
    stats: dict,
    capacity_data: list[dict],
    revenue_by_service: list[dict],
    revenue_by_client: list[dict],
) -> dict:
    """Composite 0-100 studio health score with narrative reasons."""
    score = 100
    good: list[str] = []
    watch: list[str] = []

    # ── 1. Capacity violations ────────────────────────────────────────────────
    total_emp_weeks  = sum(e["total_weeks"]     for e in capacity_data)
    total_viol_weeks = sum(e["violation_weeks"] for e in capacity_data)
    viol_rate  = total_viol_weeks / total_emp_weeks if total_emp_weeks else 0
    cap_penalty = round(viol_rate * 30)
    score -= cap_penalty
    violated_n = sum(1 for e in capacity_data if e["over_capacity"])
    if violated_n == 0:
        good.append("All staff within contracted hours — zero violations")
    else:
        watch.append(
            f"{violated_n} of {len(capacity_data)} staff exceeded contracted hours"
            f" ({total_viol_weeks} violation weeks)"
        )

    # ── 2. Budget discipline ──────────────────────────────────────────────────
    over_b  = stats["over_budget_projects"]
    total_p = stats["total_projects"]
    if over_b == 0:
        good.append(f"All {total_p} active projects delivered within hours budget")
    elif over_b == 1:
        score -= 3
        watch.append(f"{over_b} project slightly over its hours budget")
    else:
        score -= min(15, over_b * 4)
        watch.append(f"{over_b} of {total_p} projects exceeded hours budget")

    # ── 3. Service concentration ──────────────────────────────────────────────
    total_rev = stats["total_revenue"]
    if revenue_by_service:
        top = revenue_by_service[0]
        top_pct = round(top["revenue"] / total_rev * 100, 1) if total_rev else 0
        if top_pct > 50:
            score -= 12
            watch.append(
                f"'{top['service']}' = {top_pct}% of revenue — single-service concentration risk"
            )
        elif top_pct > 35:
            score -= 5
            watch.append(f"'{top['service']}' is {top_pct}% of revenue — worth diversifying")
        else:
            good.append("Healthy service mix — no single service dominates")

    # ── 4. Client concentration ───────────────────────────────────────────────
    if revenue_by_client:
        top_c     = revenue_by_client[0]
        top_c_pct = round(top_c["revenue"] / total_rev * 100, 1) if total_rev else 0
        if top_c_pct > 25:
            score -= 8
            watch.append(f"{top_c['client']} = {top_c_pct}% of revenue — key-account risk")
        else:
            good.append(f"Strong client spread — top client is only {top_c_pct}% of revenue")

    # ── 5. Blended rate strength ──────────────────────────────────────────────
    hrs = stats.get("total_hours_logged") or 1
    blended = round(total_rev / hrs)
    if blended >= 150:
        good.append(f"${blended}/hr blended rate — solid margin")

    score = max(0, min(100, round(score)))

    if score >= 80:
        label, color, bg = "Healthy",         "#16a34a", "#f0fdf4"
    elif score >= 65:
        label, color, bg = "Watch Items",     "#d97706", "#fffbeb"
    else:
        label, color, bg = "Needs Attention", "#dc2626", "#fef2f2"

    return {
        "score": score, "label": label, "color": color, "bg": bg,
        "good": good, "watch": watch,
    }


def get_projects_summary(start: str = "", end: str = "") -> list[dict]:
    """All projects with actual hours & billed amount from schedule."""
    extra_s, params_s = _date_where(start, end, table="s")
    conn = get_conn()
    # Filter schedule to date range via CTE, then join — clean single-pass params
    rows = conn.execute(f"""
        WITH sched AS (
            SELECT * FROM schedule s WHERE 1=1 {extra_s}
        )
        SELECT
            p.name, p.client, p.service,
            p.hours_budget, p.budget_usd,
            p.start_date   AS proj_start,
            p.end_date     AS proj_end,
            COALESCE(SUM(sched.hours),  0)              AS actual_hours,
            COALESCE(SUM(sched.amount), 0)              AS billed_amount,
            COUNT(DISTINCT sched.employee_name)         AS team_size
        FROM projects p
        LEFT JOIN sched ON sched.project = p.name
        GROUP BY p.name
        ORDER BY actual_hours DESC
    """, params_s).fetchall()
    conn.close()
    # normalise key names so templates see start_date / end_date
    result = []
    for r in rows:
        d = dict(r)
        d["start_date"] = d.pop("proj_start", None)
        d["end_date"]   = d.pop("proj_end",   None)
        result.append(d)
    return result


# ── Smart-action queries ─────────────────────────────────────────────────────

def get_project_health() -> list[dict]:
    """Per-project budget health with risk classification.

    Returns one row per project (only those with a hours budget) containing:
    - budget_pct    : actual / budget * 100  (over 100 = blown)
    - remaining_h   : hours left in budget   (negative = over)
    - risk          : 'OVER' | 'AT_RISK' | 'OK' | 'T&M'
    - this_week_h   : hours logged in the most-recent data week
    """
    conn = get_conn()

    # Most-recent week boundaries from the data itself
    latest = conn.execute(
        "SELECT MAX(start_date) FROM schedule"
    ).fetchone()[0] or ""
    if latest:
        hi            = datetime.strptime(latest, "%Y-%m-%d")
        week_mon      = (hi - timedelta(days=hi.weekday())).strftime("%Y-%m-%d")
        week_sun      = (hi - timedelta(days=hi.weekday()) + timedelta(days=6)).strftime("%Y-%m-%d")
    else:
        week_mon = week_sun = ""

    rows = conn.execute("""
        SELECT
            p.name, p.client, p.service,
            p.hours_budget,
            p.budget_usd,
            COALESCE(SUM(s.hours),  0) AS actual_hours,
            COALESCE(SUM(s.amount), 0) AS billed_amount,
            COALESCE(SUM(CASE WHEN s.start_date >= ? AND s.start_date <= ?
                             THEN s.hours ELSE 0 END), 0) AS this_week_h,
            COUNT(DISTINCT s.employee_name)               AS assigned_staff
        FROM projects p
        LEFT JOIN schedule s ON s.project = p.name
        GROUP BY p.name
        ORDER BY p.hours_budget DESC
    """, (week_mon, week_sun)).fetchall()
    conn.close()

    result = []
    for r in rows:
        budget_h = r["hours_budget"]
        actual_h = r["actual_hours"]
        if budget_h and budget_h > 0:
            remaining_h = round(budget_h - actual_h, 1)
            budget_pct  = round(actual_h / budget_h * 100, 1)
            if budget_pct > 100:
                risk = "OVER"
            elif budget_pct > 80:
                risk = "AT_RISK"
            else:
                risk = "OK"
        else:
            remaining_h = None
            budget_pct  = None
            risk        = "T&M"

        result.append({
            "name":          r["name"],
            "client":        r["client"],
            "service":       r["service"],
            "hours_budget":  budget_h,
            "budget_usd":    r["budget_usd"],
            "actual_hours":  round(actual_h, 1),
            "billed_amount": round(r["billed_amount"], 2),
            "remaining_h":   remaining_h,
            "budget_pct":    budget_pct,
            "risk":          risk,
            "this_week_h":   round(r["this_week_h"], 1),
            "assigned_staff": r["assigned_staff"],
        })
    return result


def get_staff_week_availability() -> list[dict]:
    """Free capacity per team member for the most-recent week in the data.

    Returns list sorted by free_hours descending (most-available first).
    """
    conn = get_conn()

    latest = conn.execute(
        "SELECT MAX(start_date) FROM schedule"
    ).fetchone()[0] or ""
    if latest:
        hi       = datetime.strptime(latest, "%Y-%m-%d")
        week_mon = (hi - timedelta(days=hi.weekday())).strftime("%Y-%m-%d")
        week_sun = (hi - timedelta(days=hi.weekday()) + timedelta(days=6)).strftime("%Y-%m-%d")
    else:
        week_mon = week_sun = ""

    employees = conn.execute(
        "SELECT name, employee_type, bill_rate, capacity_pct FROM employees"
    ).fetchall()
    week_rows = conn.execute(
        "SELECT employee_name, project, client, SUM(hours) AS hrs "
        "FROM schedule WHERE start_date >= ? AND start_date <= ? "
        "GROUP BY employee_name, project",
        (week_mon, week_sun),
    ).fetchall()
    conn.close()

    # Hours per employee this week
    week_hrs: dict[str, float] = defaultdict(float)
    week_projects: dict[str, list[str]] = defaultdict(list)
    for r in week_rows:
        week_hrs[r["employee_name"]]     += r["hrs"]
        week_projects[r["employee_name"]].append(r["project"])

    result = []
    for emp in employees:
        contracted_h    = round(emp["capacity_pct"] * 40, 1)
        scheduled_h     = round(week_hrs.get(emp["name"], 0), 1)
        free_h          = round(contracted_h - scheduled_h, 1)
        utilization_pct = round(scheduled_h / contracted_h * 100) if contracted_h else 0
        is_bench        = emp["employee_type"] == "Available Bandwidth"
        result.append({
            "name":            emp["name"],
            "employee_type":   emp["employee_type"],
            "bill_rate":       emp["bill_rate"],
            "contracted_h":    contracted_h,
            "scheduled_h":     scheduled_h,
            "free_h":          free_h,
            "utilization_pct": utilization_pct,
            "overloaded":      free_h < 0,
            "has_capacity":    free_h > 1,     # >1h truly free
            "is_bench":        is_bench,        # Available Bandwidth type — always prefer first
            "current_projects": week_projects.get(emp["name"], []),
            "week_start":       week_mon,
            "week_end":         week_sun,
        })

    # Sort order: bench first (always free to assign), then most-free, then fewest projects
    result.sort(key=lambda x: (
        0 if x["is_bench"] else 1,          # bench always floats to top
        -x["free_h"],                        # then most free hours
        len(x["current_projects"]),           # then fewest active projects
    ))
    return result
