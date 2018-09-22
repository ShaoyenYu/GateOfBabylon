import os
import pandas as pd
import shutil
import sys
import re
import pymysql
import time
from openpyxl import load_workbook


def read_csv(path, header="infer"):
    try:
        df = pd.read_csv(path, encoding="gbk", header=header)
        return df
    except Exception as e:
        print(e)


def sql_cols(df, usage="sql"):
    cols = tuple(df.columns)
    if usage == "sql":
        cols_str = str(cols).replace("'", "`")
        if len(df.columns) == 1:
            cols_str = cols_str[:-2] + ")"  # to process dataframe with only one column
        return cols_str
    elif usage == "format":
        base = "'%%(%s)s'" % cols[0]
        for col in cols[1:]:
            base += ", '%%(%s)s'" % col
        return base
    elif usage == "values":
        base = "%s=VALUES(%s)" % (cols[0], cols[0])
        for col in cols[1:]:
            base += ", `%s`=VALUES(`%s`)" % (col, col)
        return base


def to_sql(tb_name, conn, dataframe, type="update", chunksize=2000, debug=False):
    """
    Dummy of pandas.to_sql, support "REPLACE INTO ..." and "INSERT ... ON DUPLICATE KEY UPDATE (keys) VALUES (values)"
    SQL statement.

    Args:
        tb_name: str
            Table to insert get_data;
        conn:
            DBAPI Instance
        dataframe: pandas.DataFrame
            Dataframe instance
        type: str, optional {"update", "replace", "ignore"}, default "update"
            Specified the way to update get_data. If "update", then `conn` will execute "INSERT ... ON DUPLICATE UPDATE ..."
            SQL statement, else if "replace" chosen, then "REPLACE ..." SQL statement will be executed; else if "ignore" chosen,
            then "INSERT IGNORE ..." will be excuted;
        chunksize: int
            Size of records to be inserted each time;
        **kwargs:

    Returns:
        None
    """
    tb_name = ".".join([f"`{x}`" for x in tb_name.split(".")])

    df = dataframe.copy(deep=False)
    # todo 20180922 can directly fillna with NULL, need to add test
    df = df.fillna("None")
    df = df.applymap(lambda x: re.sub('([\'\"\\\])', '\\\\\g<1>', str(x)))
    cols_str = sql_cols(df)
    sqls = []
    for i in range(0, len(df), chunksize):
        df_tmp = df[i: i + chunksize]

        if type == "replace":
            sql_base = "REPLACE INTO {tb_name} {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )

        elif type == "update":
            sql_base = "INSERT INTO {tb_name} {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )
            sql_update = "ON DUPLICATE KEY UPDATE {0}".format(
                sql_cols(df_tmp, "values")
            )

        elif type == "ignore":
            sql_base = "INSERT IGNORE INTO {tb_name} {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )

        sql_val = sql_cols(df_tmp, "format")
        vals = tuple([sql_val % x for x in df_tmp.to_dict("records")])
        sql_vals = "VALUES ({x})".format(x=vals[0])
        for i in range(1, len(vals)):
            sql_vals += ", ({x})".format(x=vals[i])
        sql_vals = sql_vals.replace("'None'", "NULL")

        sql_main = sql_base + sql_vals
        if type == "update":
            sql_main += sql_update

        if sys.version_info.major == 2:
            sql_main = sql_main.replace("u`", "`")
        if sys.version_info.major == 3:
            sql_main = sql_main.replace("%", "%%")

        if debug is False:
            try:
                conn.execute(sql_main)
            except pymysql.err.InternalError as e:
                print("ENCOUNTERING ERROR: {e}, RETRYING".format(e=e))
                time.sleep(10)
                conn.execute(sql_main)
        else:
            sqls.append(sql_main)
    if debug:
        return sqls


def delete(tb_name, conn, dataframe, chunksize=10000):
    """

    Args:
        tb_name:
        conn:
        dataframe:
        chunksize:

    Returns:

    """
    dataframe = dataframe.dropna()
    for i in range(0, len(dataframe), chunksize):
        df = dataframe[i: i + chunksize]
        condition = generate_condition(df)
        sql = "DELETE FROM {tb} WHERE ({criterion})".format(
            tb=tb_name, criterion=condition
        )
        conn.execute(sql)


def generate_condition(dataframe):
    dataframe = dataframe.dropna()
    cols = dataframe.columns
    if len(cols) == 1:
        tmp = str(tuple(dataframe[cols[0]].apply(lambda x: str(x)).tolist()))
        if len(dataframe) == 1:
            tmp = tmp[:-2] + ")"
        condition = "{col} IN {val}".format(col=cols[0], val=tmp)
    else:
        s = ""
        for i, col in enumerate(cols):
            if i > 0:
                s += " AND `{k}` = ".format(k=col) + "'{" + str(i) + "}'"
            else:
                s = "`{k}` = ".format(k=col) + "'{" + str(i) + "}'"
        s = "(" + s + ")"
        conditions = []
        for val in dataframe.as_matrix():
            tmp = s.format(*val)
            conditions.append(tmp)

        condition = " OR ".join(conditions)
    return condition


def get_filenames(relative_path):
    try:
        file_names = list(os.walk(relative_path))
        return file_names[0][2]
    except Exception as e:
        pass


def check_filetype(file_path):
    file_suffix = file_path[-4:]
    if file_suffix == ".csv":
        return "spot"
    elif file_suffix == ".txt":
        return "future"
    else:
        raise TypeError("Unknown File Type")


def export_to_xl(df_dict, file_name="ddls", path=os.path.join(os.path.expanduser("~"), 'Desktop')):
    file_path = os.path.join(path, file_name)
    if ".xlsx" not in file_path.lower():
        file_path += ".xlsx"
    tmp = pd.DataFrame()
    tmp.to_excel(file_path, index=False)

    dict_items = sorted(df_dict.items(), key=lambda d: d[0], reverse=False)

    for k, v in dict_items:
        book = load_workbook("{path}".format(path=file_path))
        writer = pd.ExcelWriter("{path}".format(path=file_path), engine='openpyxl')
        writer.book = book
        writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
        v = v.fillna("")
        v = v.astype(str)
        v.to_excel(writer, "{tb_name}".format(tb_name=k, index=False), index=False)
        writer.save()


def move_file(file, folder_tgt, suffix=0):
    if os.path.isdir(folder_tgt) is False:
        os.mkdir(folder_tgt)

    if os.path.isfile(file):
        file_name = os.path.split(file)[1]

    file_type = file_name.split(".")[-1]

    new_name = os.path.join(folder_tgt, file_name)
    while os.path.isfile(new_name):
        suffix += 1
        new_name = os.path.join(
            folder_tgt,
            "{fn}({sfx}).{ft}".format(
                fn=file_name[:-(len(file_type) + 1)],
                sfx=suffix,
                ft=file_type
            )
        )

    shutil.copy(file, new_name)
    os.remove(file)
