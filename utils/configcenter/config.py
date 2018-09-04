from sqlalchemy import create_engine

default_engine = create_engine("mysql+pymysql://yu_script:15901622959q@127.0.0.1:3306/babylon", connect_args={"charset": "utf8"}, pool_size=20)
token = "a3a0919479f5cda6382245a184c405856f805e090529c1bed7c31036"
