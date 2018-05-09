
def sqlfmt(seq, fmt="cond"):
    if fmt == "cond":
        return ', '.join([f'\'{x}\'' for x in seq])
    elif fmt == "col":
        return ', '.join([f'`{x}`' for x in seq])
