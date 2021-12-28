import pymysql
class DBConn:
    
    def __init__(self, host, user, password, port, db):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db = db

    def create_db(self):
        connection = pymysql.connect(
                host = self.host,
                user = self.user,
                password = self.password,
                port = self.port
            )
        try:
            with connection.cursor() as cur:
                cur.execute(f'DROP DATABASE IF EXISTS {self.db}')
                cur.execute(f'CREATE DATABASE {self.db} CHARACTER SET utf8')
        finally:
            connection.close()

    def get_connection(self):
        connection = pymysql.connect(
                host = self.host,
                user = self.user,
                password = self.password,
                port = self.port,
                database = self.db
            )
        return connection

    def make_table(self, tablename: str, schema: dict, primary_key: str, auto_increment=False):
        '''
        Create table if it doesn't already exist
        :param tablename: Name of table
        :param schema: dict of format {<column_name> : <data_type>}
        '''
        connection = self.get_connection()
        try:
            with connection.cursor() as cursor:
                fields = [f"{field_name} {field_type}" for field_name, field_type in schema.items()]
                if auto_increment == True:
                    sql = f"DROP TABLE IF EXISTS {tablename}; CREATE TABLE {tablename}(_id INT NOT NULL AUTO_INCREMENT," + ", ".join(fields) + ", PRIMARY KEY (" + primary_key + "))"
                else:
                    sql = f"DROP TABLE IF EXISTS {tablename}; CREATE TABLE {tablename}(" + ", ".join(fields) + ", PRIMARY KEY (" + primary_key + "))"
                for statement in sql.split(';'):
                    cursor.execute(statement)
                connection.commit()
        finally:
            connection.close()