import MySQLdb

class Connsync(object):
    def get_mysql_connection(self, host, user, passwd):
        conn = None
        try:
            conn = MySQLdb.Connect(host, user, passwd)
        except Exception,e:
            raise e
        return conn

    def exc_mysql_sql(self, conn, sqlstr):
        cursor = conn.cursor()
        cursor.execute(sqlstr)
        rows = cursor.fetchall()
        return rows
