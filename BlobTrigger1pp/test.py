import pymysql


def mysqlcon():
    connection = pymysql.Connect(host=database_address, user=database_user, password=database_passwd, database='OG_auth', cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    return connection.cursor()

def collect_oauth(env):
    cursor = mysqlcon()
    # Read a single record
    sql = "QUERY"
    cursor.execute(sql)
    result = cursor.fetchall()
    print(result)
    return result

def insert_database(at,rt,env):
    cursor = mysqlcon()
    # Create a new record
    sql = "UPDATE `zohoauth` SET `accesstoken`=%s,`refreshtoken`=%s WHERE `type`=%s"
    cursor.execute(sql, (at,rt,env))