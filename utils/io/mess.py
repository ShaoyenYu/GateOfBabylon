import os
import pandas as pd


def get_filenames(relative_path):
    try:
        file_names = list(os.walk(relative_path))
        return file_names[0][2]
    except Exception as e:
        pass


def export_to_xl(df_dict, file_name="ddls", path=os.path.join(os.path.expanduser("~"), 'Desktop')):
    from openpyxl import load_workbook
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
