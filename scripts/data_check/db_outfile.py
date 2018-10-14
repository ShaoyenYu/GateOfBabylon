import pandas as pd
from sqlalchemy import create_engine
from multiprocessing.dummy import Pool as ThreadPool


def out(engine, sql):
    engine.execute(sql)


def main():
    default_engine = create_engine("mysql+pymysql://root:15901622959qxmt@127.0.0.1:3306/babylon",
                                   connect_args={"charset": "utf8"}, pool_size=0)
    df_tables = pd.read_sql("SHOW TABLES;", default_engine)
    df_tables = df_tables.loc[df_tables["Tables_in_babylon"].apply(
        lambda x: not x.startswith("stock_tickdata_")), "Tables_in_babylon"].tolist()

    p = ThreadPool(len(df_tables))
    for table in df_tables:
        base = f"SELECT * FROM `{table}` INTO OUTFILE 'D:/Downloads/data/bak/{table}.txt'"
        p.apply_async(out, args=(default_engine, base))
    p.close()
    p.join()


if __name__ == '__main__':
    main()
