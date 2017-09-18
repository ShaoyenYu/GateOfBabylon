import pandas as pd
from utils import config as cfg, io

engine = cfg.default_engine


def get_securityname(engine):
    with engine.connect() as conn:
        df = pd.read_sql("SELECT DISTINCT subject_id, subject_name FROM security_price", conn)
        conn.close()
    return df


df = get_securityname()
df = df.ix[df["subject_name"] != ""].drop_duplicates(subset=["subject_id"], keep="last")
df.columns = ["id", "name"]


def update_securityinfo(df, engine):
    with engine.connect() as conn:
        io.to_sql("security_info", conn, df)
        conn.close()


update_securityinfo(df, engine)
