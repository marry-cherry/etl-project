import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import time
import sys
from config import DB_URI

engine = create_engine(DB_URI)

PRIMARY_KEYS = {
    'ft_balance_f': ['on_date', 'account_rk'],
    'md_account_d': ['data_actual_date', 'account_rk'],
    'md_currency_d': ['currency_rk', 'data_actual_date'],
    'md_exchange_rate_d': ['data_actual_date', 'currency_rk'],
    'md_ledger_account_s': ['ledger_account', 'start_date'],
}

def log_etl(process_name, start_time, end_time=None, row_countt=0, commentt=""):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO logs.etl_log (process_name, start_time, end_time, row_countt, commentt)
            VALUES (:process_name, :start_time, :end_time, :row_countt, :commentt)
        """), {
            "process_name": process_name,
            "start_time": start_time,
            "end_time": end_time,
            "row_countt": row_countt,
            "commentt": commentt
        })

def load_csv(table_name, csv_path, schema='ds', date_cols=None, encoding='utf-8'):
    process_name = f"load_{table_name}"
    start = datetime.now()
    print(f"{process_name} started at {start}")
    log_etl(process_name, start, None, 0, "Started")

    # Пауза 5 секунд
    time.sleep(5)

    try:
        df = pd.read_csv(csv_path, sep=';', parse_dates=date_cols, encoding=encoding, dayfirst=True)
        df.columns = df.columns.str.lower()
        print(f"{table_name}: CSV loaded, shape = {df.shape}")

        if table_name == 'ft_posting_f':
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))
            df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False, method='multi', chunksize=1000)

        elif table_name in PRIMARY_KEYS:
            metadata = MetaData(schema=schema)
            tbl = Table(table_name, metadata, autoload_with=engine)
            keys = PRIMARY_KEYS[table_name]
            print(f"PK for {table_name}: {keys}")
            with engine.begin() as conn:
                for _, row in df.iterrows():
                    row_dict = row.dropna().to_dict()
                    insert_stmt = insert(tbl).values(row_dict)
                    update_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=keys,
                        set_={col: row_dict[col] for col in row_dict if col not in keys}
                    )
                    conn.execute(update_stmt)

        else:
            df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False, method='multi', chunksize=1000)

        # Подсчёт строк в БД
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
            count = result.scalar()

        end = datetime.now()
        log_etl(process_name, start, end, len(df), "Success")
        print(f"{process_name} finished at {end}. Rows in CSV: {len(df)}, Rows in DB: {count}")

    except Exception as e:
        end = datetime.now()
        log_etl(process_name, start, end, 0, f"ERROR: {e}")
        print(f"{process_name} failed: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(1)

    table = sys.argv[1]
    config = {
        'md_account_d':        {'path': 'data/md_account_d.csv',        'dates': ['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE']},
        'md_currency_d':       {'path': 'data/md_currency_d.csv',       'dates': ['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE'], 'encoding': 'ISO-8859-1'},
        'md_exchange_rate_d':  {'path': 'data/md_exchange_rate_d.csv',  'dates': ['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE']},
        'md_ledger_account_s': {'path': 'data/md_ledger_account_s.csv', 'dates': ['START_DATE', 'END_DATE']},
        'ft_posting_f':        {'path': 'data/ft_posting_f.csv',        'dates': ['OPER_DATE']},
        'ft_balance_f':        {'path': 'data/ft_balance_f.csv',        'dates': ['ON_DATE']}
    }

    if table not in config:
        print(f"Неизвестная таблица: {table}")
        sys.exit(1)

    cfg = config[table]
    load_csv(
        table,
        cfg['path'],
        date_cols=cfg.get('dates'),
        encoding=cfg.get('encoding', 'utf-8')
    )