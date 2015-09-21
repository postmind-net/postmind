__author__ = 'matthieu'

def mount_csv(in_file_name, out_table_name, sep=";", header=True, column_types = None, **kwargs):
    import plpy

    if sep != None:
        with open(in_file_name, "r") as fp:
            columns = fp.readline().replace("\n", "").replace("\r", "").split(sep)
    else:
        columns = ["value"]
    if header == False:
        columns = ["COL%d" %d for d in range(len(columns))]
    elif type(header) == list:
        columns = header
        header = False
    columns = map(lambda x: x.lower(), columns)
    if not column_types:
        column_types = ["text"] * len(columns)
    try:
        plpy.execute("""CREATE EXTENSION IF NOT EXISTS file_fdw;CREATE SERVER csv_server FOREIGN DATA WRAPPER file_fdw;""")
    except:
        pass
    if out_table_name.find(".") > 0:
        plpy.execute("CREATE SCHEMA IF NOT EXISTS %s;" %out_table_name.split(".")[0])
    plpy.execute("DROP FOREIGN TABLE IF EXISTS %s;" %out_table_name)
    cmd = """CREATE FOREIGN TABLE %s (%s) SERVER csv_server OPTIONS (filename '%s', format 'csv', \
    header '%s' %s);""" %(out_table_name, ",".join(["%s %s" %(columns[c], column_types[c]) for c in range(len(columns))]),
          in_file_name, "true" if header else "false", ", delimiter '%s'" %sep if sep != None else "")
    ret = plpy.execute(cmd)
    plpy.info(cmd)
    return ret

