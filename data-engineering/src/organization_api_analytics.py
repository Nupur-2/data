import json

import psycopg2
from psycopg2.extras import RealDictCursor
from aws_lambda_powertools.utilities import parameters

SCHEMA_NAME = "virginia_dev_saayam_rdbms"


SLA = {
    "target_days": 10,
    "target_hours": 240,
    "warning_days": 8.33,
    "warning_hours": 200
}


def get_default_response():
    return {
        "organization_overview": {
            "summary": {
                "total_organizations": 0,
                "non_profit_organizations": 0,
                "for_profit_organizations": 0,
                "collaborator_organizations": 0,
                "non_collaborator_organizations": 0,
                "contributor_organizations": 0,
                "non_contributor_organizations": 0
            },
            "organization_activity_trend": [],
            "organizations_by_type": [],
            "organizations_by_size": [],
            "organizations_by_location": [],
            "collaborator_distribution": [],
            "contributor_distribution": []
        }
    }
    


def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body, default=str)
    }


def get_db_connection():
    creds = json.loads(parameters.get_parameter(
        "/dev/saayam/db/Virginia/Analytics/user",
        decrypt=True,
        max_age=3600
    ))

    db_name = creds["DATABASE NAME"]

    return psycopg2.connect(
        host=creds["HOST"],
        database=db_name,
        user=creds["USERNAME"],
        password=creds["PASSWORD"],
        port=creds["PORT"],
        sslmode="require"
    )

def fetch_organizations_by_type(cursor):
    query = f"""
        SELECT
            org_type,
            COUNT(org_id) AS count
        FROM {SCHEMA_NAME}.organizations
        GROUP BY org_type
        ORDER BY org_type;
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    return [
        {
            "organization_type": row["org_type"],
            "count": int(row["count"])
        }
        for row in rows
    ]


def fetch_total_organizations(cursor):
    query = f"""
        SELECT COUNT(org_id) AS total_organizations
        FROM {SCHEMA_NAME}.organizations;
    """

    cursor.execute(query)
    row = cursor.fetchone()

    return int(row["total_organizations"]) if row and row["total_organizations"] is not None else 0


def fetch_organizations_by_size(cursor):
    query = f"""
        SELECT
            org_size,
            COUNT(org_id) AS count
        FROM {SCHEMA_NAME}.organizations
        GROUP BY org_size
        ORDER BY org_size;
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    return [
        {
            "organization_size": row["org_size"],
            "count": int(row["count"])
        }
        for row in rows
    ]

def lambda_handler(event, context):
    conn = None
    cursor = None
    response_body = get_default_response()

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            response_body["organization_overview"]["summary"]["total_organizations"] = \
            fetch_total_organizations(cursor)
        except Exception as error:
            print(f"Total organizations query failed: {error}")
            response_body["organization_overview"]["summary"]["total_organizations"] = 0

        try:
            response_body["organization_overview"]["organizations_by_size"] = \
            fetch_organizations_by_size(cursor)
        except Exception as error:
            print(f"Organization size query failed: {error}")
            response_body["organization_overview"]["organizations_by_size"] = []



        return build_response(200, response_body)

    except Exception as error:
        print(f"DB connection failed: {error}")
        return build_response(500, response_body)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2))