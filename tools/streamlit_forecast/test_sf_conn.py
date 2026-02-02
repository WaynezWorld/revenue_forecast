# tools/streamlit_forecast/test_sf_conn.py
import os, traceback
try:
    import snowflake.connector
except Exception as e:
    print("snowflake-connector not installed:", e)
    raise SystemExit(1)

def masked(v):
    if not v: return None
    return v if len(v) < 6 else v[:3] + "..." + v[-2:]

def test_conn():
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR")
    role = os.getenv("SNOWFLAKE_ROLE")
    wh = os.getenv("SNOWFLAKE_WAREHOUSE")
    db = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    print("ENV summary:")
    print(" ACCOUNT", masked(account))
    print(" USER", masked(user))
    print(" PASSWORD present?", bool(password))
    print(" AUTHENTICATOR", authenticator)
    print(" ROLE", role, "WAREHOUSE", wh, "DB", db, "SCHEMA", schema)
    if not account or not user:
        print("Missing ACCOUNT or USER - set environment and retry.")
        return
    kwargs = {
        "user": user,
        "account": account,
        "role": role,
        "warehouse": wh,
        "database": db,
        "schema": schema,
        "client_session_keep_alive": True
    }
    if password:
        kwargs["password"] = password
    elif authenticator and authenticator.lower() == "externalbrowser":
        kwargs["authenticator"] = "externalbrowser"
    else:
        print("No password and no externalbrowser configured — cannot connect.")
        return

    print("Attempting connection ...")
    try:
        ctx = snowflake.connector.connect(**{k:v for k,v in kwargs.items() if v is not None})
        cur = ctx.cursor()
        cur.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        print("Connected. Server returns:", cur.fetchone())
        cur.close()
        ctx.close()
    except Exception as e:
        print("Connection failed:")
        traceback.print_exc()

if __name__ == "__main__":
    test_conn()
