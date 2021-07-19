import operator
import re
import sys
from configparser import ConfigParser

import pandas as pd
import pymysql
from rich.console import Console
from rich.table import Table

console = Console()
table_diff_table = Table(show_header=True, header_style="bold magenta", title='diff table')
table_diff_column = Table(show_header=True, header_style="bold magenta", show_lines=True, title='diff column')
table_diff_indexes = Table(show_header=True, header_style="bold magenta", show_lines=True, title='diff indexes')

server_list = list()

db1_env = None
db1_host = None
db1_port = 3306
db1_user = None
db1_passwd = None

db2_env = None
db2_host = None
db2_port = 3306
db2_user = None
db2_passwd = None

b = False


def init_config(arg_list):
    global server_list, \
        db1_env, db1_host, db1_port, db1_user, db1_passwd, \
        db2_env, db2_host, db2_port, db2_user, db2_passwd

    server_name = arg_list[1]
    env_name1 = arg_list[2]
    env_name2 = arg_list[3]

    cfg = ConfigParser()
    cfg.read('./conf.ini')
    if server_name != 'all':
        server_list.append(server_name)
    else:
        server_list = cfg.get('base', 'servers').split(',')
    db1_env = cfg.get(env_name1, 'env')
    db1_host = cfg.get(env_name1, 'host')
    db1_port = cfg.get(env_name1, 'port')
    db1_user = cfg.get(env_name1, 'user')
    db1_passwd = cfg.get(env_name1, 'passwd')

    db2_env = cfg.get(env_name2, 'env')
    db2_host = cfg.get(env_name2, 'host')
    db2_port = cfg.get(env_name2, 'port')
    db2_user = cfg.get(env_name2, 'user')
    db2_passwd = cfg.get(env_name2, 'passwd')


def get_db1_conn(db):
    conn = pymysql.connect(host=str(db1_host), port=int(db1_port), user=str(db1_user), passwd=str(db1_passwd), db=db)
    return conn


def get_db2_conn(db):
    conn = pymysql.connect(host=str(db2_host), port=int(db2_port), user=str(db2_user), passwd=str(db2_passwd), db=db)
    return conn


def get_all_tables(conn):
    sql = "show tables;"
    df = pd.read_sql(sql=sql, con=conn)
    return df


def get_table_column(conn, table_name):
    sql = 'show columns from `%s`' % table_name
    df = pd.read_sql(sql=sql, con=conn)
    d = dict()
    for idx, row in df.iterrows():
        d[row['Field']] = row['Type']
    return d


def get_table_indexes(conn, table_name):
    sql = 'show INDEXES from `%s`' % table_name
    df = pd.read_sql(sql=sql, con=conn)
    d = dict()
    for idx, row in df.iterrows():
        d[row['Key_name']] = row['Column_name']
    return d


def get_table_column_list(conn, database):
    df = get_all_tables(conn)
    d = dict()
    for idx, row in df.iterrows():
        table_name = row['Tables_in_' + database]
        d[table_name] = get_table_column(conn, table_name)
    return d


def get_table_indexes_list(conn, database):
    df = get_all_tables(conn)
    d = dict()
    for idx, row in df.iterrows():
        table_name = row['Tables_in_' + database]
        d[table_name] = get_table_indexes(conn, table_name)
    return d


def diff_table_name(db1, dict_c1, db2, dict_c2):
    global b

    table_diff_table.add_column(db1)
    table_diff_table.add_column(db2)

    cl1_table_names = list(dict_c1.keys())
    cl2_table_names = list(dict_c2.keys())
    for cl1Name in cl1_table_names:
        if not is_in_list(cl1Name, cl2_table_names):
            b = False
            table_diff_table.add_row("[red]++ [/red]" + cl1Name, "[green]-- [/green]" + cl1Name)
            # print(db1, '中', cl1Name, '表在', db2, '中不存在')
    for cl2Name in cl2_table_names:
        if not is_in_list(cl2Name, cl1_table_names):
            b = False
            table_diff_table.add_row("[green]-- [/green]" + cl2Name, "[red]++ [/red]" + cl2Name)
            # print(db2, '中', cl2Name, '表在', db1, '中不存在')


def diff_table_column(db1, dict_t1, db2, dict_t2):
    global b

    table_diff_column.add_column("table")
    table_diff_column.add_column(db1)
    table_diff_column.add_column(db2)

    # 求表的交集
    table_names1 = set(dict_t1.keys())
    table_names2 = set(dict_t2.keys())

    tables = list(table_names1 & table_names2)

    need_print = False

    for table in tables:
        dict_c1 = dict_t1[table]
        dict_c2 = dict_t2[table]
        list_field1 = list(dict_c1.keys())
        list_field2 = list(dict_c2.keys())

        format_db1_columns = '\n'
        format_db2_columns = '\n'

        for field1 in list_field1:
            if not is_in_list(field1, list_field2):
                b = False
                need_print = True
                format_db1_columns = format_db1_columns + "[red]++ [/red]" + field1 + '\n'
                format_db2_columns = format_db2_columns + "[green]-- [/green]" + field1 + "\n"
                # print(db1, table, '表中【', field1, "】字段在", db2, "中不存在")
        for field2 in list_field2:
            if not is_in_list(field2, list_field1):
                b = False
                need_print = True
                format_db1_columns = format_db1_columns + "[green]-- [/green]" + field2 + '\n'
                format_db2_columns = format_db2_columns + "[red]++ [/red]" + field2 + '\n'
                # print(db2, table, '表中【', field2, "】字段在", db1, "中不存在")
        for field in list(set(list_field1) & set(list_field2)):
            if not is_equal_type(dict_c1[field], dict_c2[field]):
                b = False
                need_print = True
                format_db1_columns = format_db1_columns + "[purple]" + field + "[/purple] [red]" + dict_c1[
                    field] + '[/red]\n'
                format_db2_columns = format_db2_columns + '[purple]' + field + "[/purple] [red]" + dict_c2[
                    field] + '[/red]\n'
                # print("表", table, "中字段", field, "类型不同步")

        if need_print:
            table_diff_column.add_row(table, format_db1_columns, format_db2_columns)
            need_print = False


def diff_table_indexes(db1, dict_t1, db2, dict_t2):
    global b

    table_diff_indexes.add_column("table")
    table_diff_indexes.add_column(db1 + " indexes")
    table_diff_indexes.add_column(db2 + " indexes")

    # 求表的交集
    table_names1 = set(dict_t1.keys())
    table_names2 = set(dict_t2.keys())

    tables = list(table_names1 & table_names2)
    for table in tables:
        dict_indexes_1 = dict_t1[table]
        dict_indexes_2 = dict_t2[table]
        list_indexes_1 = list(dict_indexes_1.values())
        list_indexes_2 = list(dict_indexes_2.values())
        if not operator.eq(list_indexes_1, list_indexes_2):
            table_diff_indexes.add_row(table, format_dict(dict_indexes_1), format_dict(dict_indexes_2))
            # print('表 %s 索引不相同' % table)
            b = False


def format_dict(d):
    s = '\n'
    for k, v in d.items():
        s = s + '[orange]' + k + '[/orange] > [purple]' + v + '[/purple]\n'
    return s


def is_in_list(field, field_list):
    field = field.lower()
    field_list = [s.lower() for s in field_list]
    return field in field_list


def is_equal_type(type1, type2):
    type1 = type1.lower()
    type2 = type2.lower()
    if 'int' in type1 and 'int' in type2:
        type1 = re.sub(u"\\(.*?\\)|{.*?}|\\[.*?]", "", type1)
        type2 = re.sub(u"\\(.*?\\)|{.*?}|\\[.*?]", "", type2)
    return type1 == type2


def show_diff(server_name):
    global b
    conn1 = None
    conn2 = None
    try:
        database1 = server_name + db1_env
        database2 = server_name + db2_env
        b = True
        conn1 = get_db1_conn(database1)
        conn2 = get_db2_conn(database2)

        message_column_dict1 = get_table_column_list(conn1, database1)
        message_column_dict2 = get_table_column_list(conn2, database2)
        indexes_dict1 = get_table_indexes_list(conn1, database1)
        indexes_dict2 = get_table_indexes_list(conn2, database2)

        diff_table_name(database1, message_column_dict1, database2, message_column_dict2)
        diff_table_column(database1, message_column_dict1, database2, message_column_dict2)
        diff_table_indexes(database1, indexes_dict1, database2, indexes_dict2)

        if b:
            console.print("[green]>>>【通过】")
        else:
            if table_diff_table.rows:
                console.print(table_diff_table)
            if table_diff_column.rows:
                console.print(table_diff_column)
            if table_diff_indexes.rows:
                console.print(table_diff_indexes)
    finally:
        if conn1 is not None:
            conn1.close()
        if conn2 is not None:
            conn2.close()


if __name__ == '__main__':

    if len(sys.argv) != 4:
        console.print("[red]参数校验失败，输入格式：python mysql_field_diff.py [服务名称 或 all] [环境1] [环境2][/red]")
        console.print("[cyan]例如：python mysql_field_diff.py hr dev uat[/cyan]")
        exit()

    init_config(sys.argv)
    console.print('diff server is [green]' + str(server_list) + '[/green]')

    for server in server_list:
        show_diff(server)
