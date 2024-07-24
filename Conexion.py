import mysql.connector
import pandas as pd

class Conexion:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conexion = None

    def connect(self):
        self.conexion = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def execute_query(self, query):
        cursor = self.conexion.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        cursor.close()
        return pd.DataFrame(result, columns=column_names)
