from sqlalchemy import create_engine

default_engine = create_engine("mysql+pymysql://yu_script:15901622959q@192.168.1.78:3306/babylon", connect_args={"charset": "utf8"})
