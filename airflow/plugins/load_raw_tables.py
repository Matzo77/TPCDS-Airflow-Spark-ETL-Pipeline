import pandas as pd
from minio import Minio
from sqlalchemy import create_engine, text
from datetime import date
from collections import defaultdict
import os
import yaml

def export_all_tables_to_minio():
    # --- Load config
    config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../include/")
    with open(f"{config_dir}config.yaml", "r") as f:
        cfg = yaml.safe_load(f)

    # -------------------------------------------
    # 1. PostgreSQL connection
    # -------------------------------------------
    pg = cfg["postgres"]
    cfg["postgres"]["password"] = os.getenv("POSTGRES_PASSWORD")

    engine = create_engine(
    f"postgresql+psycopg2://{pg['username']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['database']}"
)

    # -------------------------------------------
    # 2. MinIO client configuration
    # -------------------------------------------
    minio_cfg = cfg["minio"]
    minio_client = Minio(
        minio_cfg["endpoint"],
        access_key = os.getenv("MINIO_ACCESS_KEY"),
        secret_key = os.getenv("MINIO_SECRET_KEY"),
        secure=minio_cfg["secure"]
    )
    
    bucket_name = minio_cfg["bucket"]
    prefix = "raw/"

    # -------------------------------------------
    # 3. Determine today's date string
    # -------------------------------------------
    today = date.today()
    today_str = f"{today.year}-{today.month:02d}-{today.day:02d}"

    # -------------------------------------------
    # 4. Delete all existing files in raw/
    # -------------------------------------------
    print("🗑 Deleting existing files in MinIO 'raw/' directory...")
    objects_to_delete = [
        obj.object_name
        for obj in minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    ]
    for obj_name in objects_to_delete:
        try:
            minio_client.remove_object(bucket_name, obj_name)
            print(f"🗑 Deleted {obj_name}")
        except Exception as e:
            print(f"⚠️ Failed to delete {obj_name}: {e}")

    # -------------------------------------------
    # 5. Determine today's date_sk from raw.date_dim
    # -------------------------------------------
    with engine.connect() as conn:
        try:
            result = conn.execute(
                text("SELECT d_date_sk, d_date FROM raw.date_dim WHERE d_date = CURRENT_DATE;")
            ).fetchone()
        except Exception as e:
            engine.dispose()
            raise RuntimeError(f"❌ Failed to query raw.date_dim for CURRENT_DATE: {e}")

        if not result:
            engine.dispose()
            raise ValueError(
                "❌ No d_date_sk found for today's date in raw.date_dim; check that raw.date_dim.d_date has CURRENT_DATE present."
            )
        today_sk = result[0]
        sample_d_date = result[1]
        print(f"✅ Today's d_date_sk = {today_sk} (d_date sample: {sample_d_date})")

    # Ensure it's an int-like value for safe SQL formatting
    try:
        today_sk_int = int(today_sk)
    except Exception:
        engine.dispose()
        raise TypeError(f"❌ today_sk value is not integer-like: {today_sk!r}")

    # -------------------------------------------
    # 6. Get all tables in raw schema
    # -------------------------------------------
    all_tables = []
    with engine.connect() as conn:
        res = conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='raw' AND table_type='BASE TABLE';"
            )
        )
        all_tables = [r[0] for r in res]
    print(f"📚 Found {len(all_tables)} tables in schema 'raw' (example: {all_tables[:10]})")

    # -------------------------------------------
    # 7. Get tables with *_date_sk columns
    # -------------------------------------------
    table_cols = defaultdict(list)
    with engine.connect() as conn:
        query_cols = text(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'raw'
                AND column_name LIKE '%_date_sk';
            """
        )
        for table_name, column_name in conn.execute(query_cols):
            table_cols[table_name].append(column_name)

    print(
        f"✅ Found {len(table_cols)} tables with *_date_sk columns in 'raw' schema: {list(table_cols.keys())[:20]}"
    )

    # -------------------------------------------
    # 8. Build date filter function (TPC-DS rules)
    # -------------------------------------------
    def build_date_filter(table_name: str, columns: list[str], today_sk: int) -> str:
        """
        Build WHERE clause for a table that filters by today's date_sk
        using TPC-DS rules.

        Rules:
        - Required dates (cannot be NULL, must be <= today): sold, open, start, creation, return
        - Optional dates (can be NULL, <= today if present): ship, close, end, closed, first_sales, first_shipto, access
        """
        conditions = []
        for col in columns:
            c = col.lower()

            if any(k in c for k in ["sold", "open", "start", "creation", "return"]):
                # Required dates
                conditions.append(f'("{col}" IS NOT NULL AND "{col}" <= {today_sk})')
            elif any(k in c for k in ["ship", "close", "end", "closed", "first_sales", "first_shipto", "access"]):
                # Optional dates
                conditions.append(f'("{col}" IS NULL OR "{col}" <= {today_sk})')
            else:
                # Fallback
                conditions.append(f'("{col}" IS NULL OR "{col}" <= {today_sk})')

        return " AND ".join(conditions)

    # -------------------------------------------
    # 9. Export function
    # -------------------------------------------
    def export_table_to_minio(table_name, columns, today_sk_val):
        """
        columns: list of column names in this table that match %_date_sk (possibly empty list)
        today_sk_val: integer value of d_date_sk for CURRENT_DATE
        """
        if table_name == "date_dim":
            query = """
                SELECT *
                FROM raw.date_dim
                WHERE d_date BETWEEN '1990-01-01' AND '2030-01-01';
            """
            reason = "special date_dim range filter"
        elif columns:
            where_clause = build_date_filter(table_name, columns, today_sk_val)
            query = f'SELECT * FROM raw."{table_name}" WHERE {where_clause};'
            reason = f"applied date_sk filter using columns: {columns}"
        else:
            query = f'SELECT * FROM raw."{table_name}";'
            reason = "no date_sk columns -> unfiltered export"

        print(f"\n➡ Exporting table: {table_name}")
        print(f"   - Decision: {reason}")
        print(f"   - Query preview: {query.strip()[:400]}")

        # Temp file path
        os.makedirs("/tmp/tpcds_exports", exist_ok=True)
        csv_file = f"/tmp/tpcds_exports/{table_name}_{today_str}.csv"

        # Object path under the configured prefix
        object_path = f"{prefix}{table_name}_{today_str}.csv"

        try:
            with engine.connect() as conn:
                df = pd.read_sql(query, conn)
                print(f"✅ {table_name}: {len(df)} rows selected.")
                df.to_csv(csv_file, index=False)

            minio_client.fput_object(bucket_name, object_path, csv_file)
            print(f"📤 Uploaded to MinIO: {object_path}")

            os.remove(csv_file)
            print(f"🧹 Removed temporary file: {csv_file}")

        except Exception as e:
            print(f"❌ Failed to export {table_name}: {e}")


    # -------------------------------------------
    # 10. Loop over all tables
    # -------------------------------------------
    for tbl in all_tables:
        cols = table_cols.get(tbl, [])
        # Ensure date_dim never uses generic date_sk logic
        if tbl == "date_dim":
            cols = []
        print(f"\n-- Preparing to export {tbl} -- columns detected for date_sk: {cols}")
        export_table_to_minio(tbl, cols, today_sk_int)

    engine.dispose()
    print(f"\n🏁 Done — all tables exported to MinIO under '{prefix}' with today's token '{today_str}'.")
