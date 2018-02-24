from util import io, config as cfg
import pandas as pd

engine = cfg.default_engine


def fetch():
    sql = "SELECT sih.* FROM stock_info_hist sih \
    JOIN (SELECT stock_id, MAX(date) as date FROM stock_info_hist GROUP BY stock_id) tb_md \
    ON sih.date = tb_md.date AND sih.stock_id = tb_md.stock_id"

    with engine.connect() as conn:
        data = pd.read_sql(sql, conn)
        conn.close()
    data = data.drop(["entry_time", "update_time"], axis=1)
    return data


def main():
    df_stock_info = fetch()
    with engine.connect() as conn:
        io.to_sql("stock_info", conn, df_stock_info)
        conn.close()


if __name__ == "__main__":
    main()
