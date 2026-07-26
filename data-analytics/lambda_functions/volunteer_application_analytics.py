import json
import psycopg2
import boto3

REAL_TABLE_STATE_VIRGINIA ="virginia_dev_saayam_rdbms.state"
REAL_TABLE_USERS_VIRGINIA ="virginia_dev_saayam_rdbms.users"
REAL_TABLE_VOLUNTEER_DETAILS_VIRGINIA ="virginia_dev_saayam_rdbms.volunteer_details"
REAL_TABLE_CITY_VIRGINIA ="virginia_dev_saayam_rdbms.city"
REAL_TABLE_USER_SKILL_VIRGINIA ="virginia_dev_saayam_rdbms.user_skills"
REAL_TABLE_VOLUNTEER_LOCATIONS_VIRGINIA ="virginia_dev_saayam_rdbms.volunteer_locations"
REAL_TABLE_USER_LOCATIONS_VIRGINIA ="virginia_dev_saayam_rdbms.user_locations"
REAL_TABLE_COUNTRY_VIRGINIA ="virginia_dev_saayam_rdbms.country"
REAL_TABLE_HELP_CATEGORIES_VIRGINIA = "virginia_dev_saayam_rdbms.help_categories"

REAL_TABLE_STATE_IRELAND ="ireland_dev_saayam_rdbms.state"
REAL_TABLE_USERS_IRELAND ="ireland_dev_saayam_rdbms.users"
REAL_TABLE_VOLUNTEER_DETAILS_IRELAND ="ireland_dev_saayam_rdbms.volunteer_details"
REAL_TABLE_CITY_IRELAND ="ireland_dev_saayam_rdbms.city"
REAL_TABLE_USER_SKILLS_IRELAND ="ireland_dev_saayam_rdbms.user_skills"
REAL_TABLE_VOLUNTEER_LOCATIONS_IRELAND ="ireland_dev_saayam_rdbms.volunteer_locations"
REAL_TABLE_USER_LOCATIONS_IRELAND ="ireland_dev_saayam_rdbms.user_locations"
REAL_TABLE_COUNTRY_IRELAND ="ireland_dev_saayam_rdbms.country"
REAL_TABLE_HELP_CATEGORIES_IRELAND = "ireland_dev_saayam_rdbms.help_categories"


# ---------------------------------------------------------------------------
# NEW: Date filter helper for Volunteer Activity Trend
# ---------------------------------------------------------------------------

def build_date_filter_trend(time_range, start_date=None, end_date=None):
    """
    Returns (sql_condition, params_tuple) for the Volunteer Activity Trend
    queries. Filters on vd.created_at.

    time_range values:
        "7D"     -> past 7 days
        "30D"    -> past 30 days
        "1Y"     -> past 1 year
        "All"    -> no filter (returns empty string and empty tuple)
        "Custom" -> between start_date and end_date (both required)
    """
    if time_range == "Custom" and start_date and end_date:
        return "vd.created_at BETWEEN %s AND %s", (start_date, end_date)
    elif time_range == "7D":
        return "vd.created_at >= CURRENT_DATE - INTERVAL '7 days'", ()
    elif time_range == "30D":
        return "vd.created_at >= CURRENT_DATE - INTERVAL '30 days'", ()
    elif time_range == "1Y":
        return "vd.created_at >= CURRENT_DATE - INTERVAL '1 year'", ()
    else:
        # "All" or anything unrecognised -> no filter
        return "", ()


# ---------------------------------------------------------------------------
# NEW: Date filter helper for Volunteers by Location
# ---------------------------------------------------------------------------

def build_date_filter_location(time_range_location, location_start_date=None, location_end_date=None):
    """
    Returns (sql_condition, params_tuple) for the Volunteers by Location query.
    Filters on vd.created_at. Independent of the trend filter.
    """
    if time_range_location == "Custom" and location_start_date and location_end_date:
        return "vd.created_at BETWEEN %s AND %s", (location_start_date, location_end_date)
    elif time_range_location == "7D":
        return "vd.created_at >= CURRENT_DATE - INTERVAL '7 days'", ()
    elif time_range_location == "30D":
        return "vd.created_at >= CURRENT_DATE - INTERVAL '30 days'", ()
    elif time_range_location == "1Y":
        return "vd.created_at >= CURRENT_DATE - INTERVAL '1 year'", ()
    else:
        return "", ()


# ---------------------------------------------------------------------------
# NEW: Grouping helper for Volunteer Activity Trend
# ---------------------------------------------------------------------------

def get_grouping(time_range):
    """
    Returns the SQL date-truncation unit and format string for grouping
    Volunteer Activity Trend data.

    7D / 30D / Custom -> daily  ("YYYY-MM-DD")
    1Y  / All         -> monthly ("YYYY-MM")
    """
    if time_range in ("7D", "30D", "Custom"):
        return "day", "YYYY-MM-DD"
    else:
        # "1Y" or "All"
        return "month", "YYYY-MM"


# ---------------------------------------------------------------------------
# Unchanged helpers
# ---------------------------------------------------------------------------

def parse_event_body(event):
    if not event:
        return {}
    body = event.get("body")
    if body is None:
        return event
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}
    if isinstance(body, dict):
        return body
    return {}


# ---------------------------------------------------------------------------
# UPDATED: lambda_handler — reads date-filter params from event
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    conn_V = None
    cursor_V = None
    conn_I = None
    cursor_I = None

    safe_response = {
        "volunteer_activity_trend": {
            "new_volunteers": [],
            "active_volunteers": [],
            "total_volunteers": []
        },
        "volunteers_by_location": []
    }

    try:
        VIRGINIA_DB_CONFIG = get_db_config('Virginia')
        IRELAND_DB_CONFIG = get_db_config('Ireland')
        conn_V = psycopg2.connect(**VIRGINIA_DB_CONFIG)
        cursor_V = conn_V.cursor()
        print("Virginia database connected successfully.")
        conn_I = psycopg2.connect(**IRELAND_DB_CONFIG)
        cursor_I = conn_I.cursor()
        print("Ireland database connected successfully.")

        request_body = parse_event_body(event)

        # Existing params (unchanged)
        country    = request_body.get("country", "All Countries")
        chart_type = request_body.get("chart_type", "Bar Chart")
        skill      = request_body.get("skill", "All Skills")

        # NEW: date-filter params for Volunteer Activity Trend
        time_range  = request_body.get("time_range", "All")
        start_date  = request_body.get("start_date", None)
        end_date    = request_body.get("end_date", None)

        # NEW: independent date-filter params for Volunteers by Location
        time_range_location    = request_body.get("time_range_location", "All")
        location_start_date    = request_body.get("location_start_date", None)
        location_end_date      = request_body.get("location_end_date", None)

        # Fetch from Virginia
        volunteer_activity_trend_virginia = get_volunteer_activity_trend(
            cursor_V,
            REAL_TABLE_USERS_VIRGINIA,
            REAL_TABLE_VOLUNTEER_DETAILS_VIRGINIA,
            time_range, start_date, end_date
        )
        volunteers_by_location_virginia = get_volunteers_by_location(
            cursor_V,
            REAL_TABLE_USERS_VIRGINIA,
            REAL_TABLE_VOLUNTEER_DETAILS_VIRGINIA,
            REAL_TABLE_COUNTRY_VIRGINIA,
            REAL_TABLE_USER_SKILL_VIRGINIA,
            REAL_TABLE_HELP_CATEGORIES_VIRGINIA,
            country, chart_type, skill,
            time_range_location, location_start_date, location_end_date
        )

        # Fetch from Ireland
        volunteer_activity_trend_ireland = get_volunteer_activity_trend(
            cursor_I,
            REAL_TABLE_USERS_IRELAND,
            REAL_TABLE_VOLUNTEER_DETAILS_IRELAND,
            time_range, start_date, end_date
        )
        volunteers_by_location_ireland = get_volunteers_by_location(
            cursor_I,
            REAL_TABLE_USERS_IRELAND,
            REAL_TABLE_VOLUNTEER_DETAILS_IRELAND,
            REAL_TABLE_COUNTRY_IRELAND,
            REAL_TABLE_USER_SKILLS_IRELAND,
            REAL_TABLE_HELP_CATEGORIES_IRELAND,
            country, chart_type, skill,
            time_range_location, location_start_date, location_end_date
        )

        response_data = {
            "volunteer_activity_trend": merge_volunteer_activity_trend(
                volunteer_activity_trend_virginia,
                volunteer_activity_trend_ireland
            ),
            "volunteers_by_location": merge_volunteer_by_location(
                volunteers_by_location_virginia,
                volunteers_by_location_ireland
            )
        }

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
            },
            "body": json.dumps(response_data)
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
            },
            "body": json.dumps(safe_response)
        }
    finally:
        if cursor_V:
            cursor_V.close()
        if conn_V:
            conn_V.close()
        print("Virginia Database connection closed")
        if cursor_I:
            cursor_I.close()
        if conn_I:
            conn_I.close()
        print("Ireland Database connection closed")


# ---------------------------------------------------------------------------
# UPDATED: get_volunteer_activity_trend — date filter + grouping + "period" key
# ---------------------------------------------------------------------------

def get_volunteer_activity_trend(cursor, users, volunteer_details,
                                  time_range="All", start_date=None, end_date=None):
    try:
        date_condition, params = build_date_filter_trend(time_range, start_date, end_date)
        trunc_unit, date_fmt   = get_grouping(time_range)

        # Build WHERE clause
        where_base     = "WHERE vd.created_at IS NOT NULL"
        date_where     = f"AND {date_condition}" if date_condition else ""
        active_where   = f"AND u.user_status_id = 1"

        # Query 1: new volunteers
        query1 = f"""
            SELECT TO_CHAR(DATE_TRUNC('{trunc_unit}', vd.created_at), '{date_fmt}') AS period,
                   COUNT(DISTINCT u.user_id) AS count
            FROM {users} u
            JOIN {volunteer_details} vd ON u.user_id = vd.user_id
            {where_base}
            {date_where}
            GROUP BY 1
            ORDER BY 1 ASC
        """
        cursor.execute(query1, params)
        new_volunteers_final = [
            {"period": row[0], "count": int(row[1])}
            for row in cursor.fetchall()
        ]

        # Query 2: active volunteers
        query2 = f"""
            SELECT TO_CHAR(DATE_TRUNC('{trunc_unit}', vd.created_at), '{date_fmt}') AS period,
                   COUNT(DISTINCT u.user_id) AS count
            FROM {users} u
            JOIN {volunteer_details} vd ON u.user_id = vd.user_id
            {where_base}
            {date_where}
            {active_where}
            GROUP BY 1
            ORDER BY 1 ASC
        """
        cursor.execute(query2, params)
        active_volunteers_final = [
            {"period": row[0], "count": int(row[1])}
            for row in cursor.fetchall()
        ]

        # Query 3: cumulative total volunteers
        query3 = f"""
            SELECT period, SUM(count) OVER (ORDER BY period) AS count
            FROM (
                SELECT TO_CHAR(DATE_TRUNC('{trunc_unit}', vd.created_at), '{date_fmt}') AS period,
                       COUNT(DISTINCT u.user_id) AS count
                FROM {users} u
                JOIN {volunteer_details} vd ON u.user_id = vd.user_id
                {where_base}
                {date_where}
                GROUP BY 1
            ) sub
            ORDER BY period ASC
        """
        cursor.execute(query3, params)
        total_volunteers_final = [
            {"period": row[0], "count": int(row[1])}
            for row in cursor.fetchall()
        ]

        return {
            "new_volunteers":    new_volunteers_final,
            "active_volunteers": active_volunteers_final,
            "total_volunteers":  total_volunteers_final
        }

    except Exception as e:
        print("Error in get_volunteer_activity_trend:", str(e))
        return {
            "new_volunteers":    [],
            "active_volunteers": [],
            "total_volunteers":  []
        }


# ---------------------------------------------------------------------------
# UPDATED: merge helpers — use "period" key instead of "month"
# ---------------------------------------------------------------------------

def merge_period_data(list1, list2):
    """Merge two lists of {period, count} dicts by summing counts per period."""
    merged = {}
    for row in list1 + list2:
        period = row["period"]
        merged[period] = merged.get(period, 0) + row["count"]
    return [
        {"period": period, "count": merged[period]}
        for period in sorted(merged.keys())
    ]


def merge_volunteer_activity_trend(va_virginia, va_ireland):
    return {
        "new_volunteers": merge_period_data(
            va_virginia.get("new_volunteers", []),
            va_ireland.get("new_volunteers", [])
        ),
        "active_volunteers": merge_period_data(
            va_virginia.get("active_volunteers", []),
            va_ireland.get("active_volunteers", [])
        ),
        "total_volunteers": merge_period_data(
            va_virginia.get("total_volunteers", []),
            va_ireland.get("total_volunteers", [])
        )
    }


def merge_volunteer_by_location(list1, list2):
    merged = {}
    for row in list1 + list2:
        country = row["country"]
        merged[country] = merged.get(country, 0) + row["count"]
    return [
        {"country": country, "count": merged[country]}
        for country in sorted(merged.keys())
    ]


# ---------------------------------------------------------------------------
# UPDATED: get_volunteers_by_location — date filter added, no grouping
# ---------------------------------------------------------------------------

def get_volunteers_by_location(cursor, users, volunteer_details, country_table,
                                user_skills, help_categories,
                                country="All Countries", chart_type="Bar Chart",
                                skill="All Skills",
                                time_range_location="All",
                                location_start_date=None, location_end_date=None):
    try:
        date_condition, date_params = build_date_filter_location(
            time_range_location, location_start_date, location_end_date
        )

        query = f"""
            SELECT
                COALESCE(c.country_code, 'Unknown') AS country,
                COUNT(DISTINCT u.user_id) AS count
            FROM {users} u
            JOIN {volunteer_details} vd ON u.user_id = vd.user_id
            LEFT JOIN {country_table} c ON u.country_id = c.country_id
            WHERE 1=1
        """

        params = list(date_params)

        # Apply date filter
        if date_condition:
            query += f" AND {date_condition}"

        # Existing country filter (unchanged)
        if country != "All Countries":
            query += " AND UPPER(c.country_code) = %s"
            params.append(country)

        # Existing skill filter (unchanged)
        if skill != "All Skills":
            query += f"""
            AND EXISTS (
                SELECT 1
                FROM {user_skills} us
                JOIN {help_categories} h ON us.cat_id = h.cat_id
                WHERE us.user_id = u.user_id
                AND h.cat_name = %s
            )"""
            params.append(skill)

        query += """
            GROUP BY COALESCE(c.country_code, 'Unknown')
            ORDER BY count DESC;
        """

        cursor.execute(query, params)
        rows = cursor.fetchall()

        return [
            {"country": row[0], "count": int(row[1])}
            for row in rows
        ]

    except Exception as e:
        print("Error in get_volunteers_by_location:", str(e))
        return []


# ---------------------------------------------------------------------------
# Unchanged: get_db_config
# ---------------------------------------------------------------------------

def get_db_config(db):
    ssm = boto3.client("ssm", region_name="us-east-1")

    if db == "Virginia":
        parameter_name = "/dev/saayam/db/Virginia/Analytics/user"
    elif db == "Ireland":
        parameter_name = "/dev/saayam/db/Ireland/Analytics/user"
    else:
        raise ValueError("Database must be either Virginia or Ireland")

    response = ssm.get_parameter(
        Name=parameter_name,
        WithDecryption=True
    )

    config = response["Parameter"]["Value"]
    config_list = [line.strip() for line in config.splitlines()]

    host     = config_list[1].split()[1][1:-2]
    port     = int(config_list[5].split()[1][:-1])
    dbname   = config_list[4].split()[2][1:-2]
    user     = config_list[2].split()[1][1:-2]
    password = config_list[3].split()[1][1:-2]

    return {
        "host":     host,
        "port":     port,
        "dbname":   dbname,
        "user":     user,
        "password": password
    }


if __name__ == "__main__":
    """
    Local testing using CSV files from data-analytics/sql/.
    Reads users.csv and volunteer_details.csv into a local SQLite DB
    so the queries run without real AWS/DB access.

    Run from the repo root:
        python data-analytics/lambda_functions/volunteer_application_analytics.py

    Install dependencies first:
        pip install psycopg2-binary pandas
    """
    import os
    import sqlite3
    import pandas as pd

    # ---------- locate the sql/ folder relative to this file ----------
    THIS_DIR = os.path.dirname(os.path.abspath(__file__))
    SQL_DIR  = os.path.join(THIS_DIR, "..", "..", "data-analytics", "sql")
    # If running from repo root, override:
    if not os.path.isdir(SQL_DIR):
        SQL_DIR = os.path.join("data-analytics", "sql")

    users_csv   = os.path.join(SQL_DIR, "users.csv")
    details_csv = os.path.join(SQL_DIR, "volunteer_details.csv")

    if not os.path.isfile(users_csv) or not os.path.isfile(details_csv):
        print(f"CSV files not found in {SQL_DIR}.")
        print("Run from the repo root or adjust SQL_DIR above.")
        exit(1)

    # ---------- load CSVs into an in-memory SQLite database ----------
    users_df   = pd.read_csv(users_csv)
    details_df = pd.read_csv(details_csv)

    sqlite_conn = sqlite3.connect(":memory:")
    users_df.to_sql("users",              sqlite_conn, index=False, if_exists="replace")
    details_df.to_sql("volunteer_details", sqlite_conn, index=False, if_exists="replace")

    # SQLite cursor (uses positional ? placeholders; swap %s -> ? for local run)
    sqlite_cursor = sqlite_conn.cursor()

    # ---------- helper: adapt psycopg2-style %s params to SQLite ? ----------
    class LocalCursor:
        """Thin wrapper so the same query functions work with SQLite."""
        def __init__(self, cur):
            self._cur = cur
            self._rows = []

        def execute(self, query, params=()):
            q = query.replace("%s", "?")
            # SQLite doesn't support DATE_TRUNC / TO_CHAR — replace with strftime
            q = q.replace("DATE_TRUNC('day',",   "DATE(")
            q = q.replace("DATE_TRUNC('month',",  "DATE(")
            q = q.replace("TO_CHAR(DATE(",        "strftime('%Y-%m-%d', DATE(")
            q = q.replace("TO_CHAR(DATE_TRUNC",   "strftime('%Y-%m', DATE_TRUNC")
            q = q.replace(", 'YYYY-MM-DD')",      ")")
            q = q.replace(", 'YYYY-MM')",         ")")
            q = q.replace("CURRENT_DATE",         "DATE('now')")
            q = q.replace("INTERVAL '7 days'",    "7")
            q = q.replace("INTERVAL '30 days'",   "30")
            q = q.replace("INTERVAL '1 year'",    "365")
            self._cur.execute(q, params)
            self._rows = self._cur.fetchall()

        def fetchall(self):
            return self._rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

    local_cursor = LocalCursor(sqlite_cursor)

    # ---------- define local table names (SQLite has no schema prefix) ----------
    USERS            = "users"
    VOLUNTEER_DETAILS = "volunteer_details"

    # ---------- run all 5 test scenarios ----------
    test_scenarios = [
        ("Default (All)",  "All",    None,         None),
        ("7D",             "7D",     None,         None),
        ("30D",            "30D",    None,         None),
        ("1Y",             "1Y",     None,         None),
        ("Custom",         "Custom", "2026-01-01", "2026-12-31"),
    ]

    print("=" * 60)
    print("LOCAL TESTING — volunteer_application_analytics.py")
    print(f"Using CSV files from: {os.path.abspath(SQL_DIR)}")
    print("=" * 60)

    for label, time_range, start_date, end_date in test_scenarios:
        print(f"\n--- Scenario: {label} ---")
        result = get_volunteer_activity_trend(
            local_cursor, USERS, VOLUNTEER_DETAILS,
            time_range, start_date, end_date
        )
        print("new_volunteers    :", result["new_volunteers"][:3], "...")
        print("active_volunteers :", result["active_volunteers"][:3], "...")
        print("total_volunteers  :", result["total_volunteers"][:3], "...")

    print("\n--- Location filter scenarios ---")
    location_scenarios = [
        ("All",    None,         None),
        ("7D",     None,         None),
        ("30D",    None,         None),
        ("1Y",     None,         None),
        ("Custom", "2026-01-01", "2026-12-31"),
    ]

    # For location we need a country table — use users as a stand-in
    # (country_code will be NULL since SQLite has no country table)
    users_df["country_code"] = "UNKNOWN"
    users_df.to_sql("country", sqlite_conn, index=False, if_exists="replace")

    for label, loc_start, loc_end in location_scenarios:
        print(f"\n  Location {label}:")
        result = get_volunteers_by_location(
            local_cursor, USERS, VOLUNTEER_DETAILS, "country",
            USERS, USERS,
            time_range_location=label,
            location_start_date=loc_start,
            location_end_date=loc_end
        )
        print("  volunteers_by_location:", result[:3])

    sqlite_conn.close()
    print("\n" + "=" * 60)
    print("LOCAL TESTING COMPLETE")
    print("=" * 60)