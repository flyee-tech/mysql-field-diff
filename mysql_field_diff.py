import sys
from configparser import ConfigParser

import pandas as pd
import pymysql

server_list = list()

db1_env = None
db1_host = None
db1_user = None
db1_passwd = None

db2_env = None
db2_host = None
db2_user = None
db2_passwd = None


def init_config(arg_list):
    global server_list, db1_env, db1_host, db1_user, db1_passwd, db2_env, db2_host, db2_user, db2_passwd

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
    db1_user = cfg.get(env_name1, 'user')
    db1_passwd = cfg.get(env_name1, 'passwd')

    db2_env = cfg.get(env_name2, 'env')
    db2_host = cfg.get(env_name2, 'host')
    db2_user = cfg.get(env_name2, 'user')
    db2_passwd = cfg.get(env_name2, 'passwd')


def get_db1_conn(db):
    conn = pymysql.connect(host=str(db1_host), port=3306, user=str(db1_user), passwd=str(db1_passwd), db=db)
    return conn


def get_db2_conn(db):
    conn = pymysql.connect(host=str(db2_host), port=3306, user=str(db2_user), passwd=str(db2_passwd), db=db)
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


def get_table_column_list(conn, database):
    df = get_all_tables(conn)
    d = dict()
    for idx, row in df.iterrows():
        table_name = row['Tables_in_' + database]
        d[table_name] = get_table_column(conn, table_name)
    return d


def diff_table_name(db1, dict_c1, db2, dict_c2):
    global b
    cl1_table_names = list(dict_c1.keys())
    cl2_table_names = list(dict_c2.keys())
    for cl1Name in cl1_table_names:
        if cl1Name not in cl2_table_names:
            b = False
            print(db1, '中', cl1Name, '表在', db2, '中不存在')
    for cl2Name in cl2_table_names:
        if cl2Name not in cl1_table_names:
            b = False
            print(db2, '中', cl2Name, '表在', db1, '中不存在')


def diff_table_column(db1, dict_t1, db2, dict_t2):
    global b
    # 求表的交集
    table_names1 = set(dict_t1.keys())
    table_names2 = set(dict_t2.keys())

    tables = list(table_names1 & table_names2)

    for table in tables:
        dict_c1 = dict_t1[table]
        dict_c2 = dict_t2[table]
        list_field1 = list(dict_c1.keys())
        list_field2 = list(dict_c2.keys())

        for field1 in list_field1:
            if field1 not in list_field2:
                b = False
                print(db1, table, '表中【', field1, "】字段在", db2, "中不存在")
        for field2 in list_field2:
            if field2 not in list_field1:
                b = False
                print(db2, table, '表中【', field2, "】字段在", db1, "中不存在")
        for field in list(set(list_field1) & set(list_field2)):
            if dict_c1[field] != dict_c2[field]:
                b = False
                print("表", table, "中字段", field, "类型不同步")


def show_diff(server_name):
    global b
    conn1 = None
    conn2 = None
    try:
        database1 = server_name + db1_env
        database2 = server_name + db2_env
        b = True
        print("\n------------------------start------------------------")
        print(">>>【", database1, "】 diff 【", database2, "】")
        conn1 = get_db1_conn(database1)
        conn2 = get_db2_conn(database2)

        message_column_dict1 = get_table_column_list(conn1, database1)
        message_column_dict2 = get_table_column_list(conn2, database2)

        diff_table_name(database1, message_column_dict1, database2, message_column_dict2)
        diff_table_column(database1, message_column_dict1, database2, message_column_dict2)

        if b:
            print(">>>【通过】")
        print("------------------------end--------------------------\n")
    finally:
        if conn1 is not None:
            conn1.close()
        if conn2 is not None:
            conn2.close()


if __name__ == '__main__':

    if len(sys.argv) != 4:
        print("参数校验失败，输入格式：python mysql_field_diff.py [服务名称 或 all] [环境1] [环境2]")
        print("例如：python mysql_field_diff.py hr dev uat")
        exit()

    init_config(sys.argv)
    print('server_list =', server_list)
    print('db1_env =', db1_env)
    print('db2_env =', db2_env)

    for server in server_list:
        show_diff(server)
