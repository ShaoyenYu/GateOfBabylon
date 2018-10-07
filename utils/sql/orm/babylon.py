from sqlalchemy import Column, String, TIMESTAMP, DECIMAL, INTEGER, Enum, DATE
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StockInfo(Base):
    stock_id = Column("stock_id", String, primary_key=True)
    date = Column("date", DATE)


class StockTickdata:
    stock_id = Column("stock_id", String, primary_key=True)
    time = Column("time", TIMESTAMP, primary_key=True)
    price = Column("price", DECIMAL)
    change = Column("change", DECIMAL)
    volume = Column("volume", INTEGER)
    amount = Column("amount", INTEGER)
    type_ = Column("type", Enum("b", "s", "m"))


def __register():
    for year, month in ((y, m) for y in (2017, 2018) for m in range(1, 13)):
        y, m = str(year), f"{str(month).zfill(2)}"
        cls = f"StockTickData{y}{m}Sina"

        # Define StockTickdataYYYYMMSina classes dynamically
        globals()[cls] = type(cls, (Base, StockTickdata), {"__tablename__": f"stock_tickdata_{y}{m}_sina"})


__register()
