import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv()

DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_PORT = int(os.getenv('DB_PORT', '8123'))  # Convert to int with default
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'default')
DB = os.getenv('DB', 'default')


class DBInstance():
    def __init__(self,
                host: str = DB_HOST,
                port: int = DB_PORT,
                username: str = DB_USER,
                password: str = DB_PASSWORD,
                database: str = DB):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database    
    
    def get_client(self):
        """
        Create and return a ClickHouse client connection.

        Args:
            self
        Returns:
            clickhouse_connect.driver.Client: Connected ClickHouse client
        """
        client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database
        )
        return client

if __name__ == '__main__':
    db = DBInstance()

    try:
        client = db.get_client()
        print("Client initialised")

        try: 
            client.command("""
                CREATE TABLE IF NOT EXISTS new_table 
                    (key UInt32, 
                    value String, 
                    metric Float64) 
                ENGINE MergeTree 
                ORDER BY key""")
            row1 = [1000, 'String Value 1000', 5.233]
            row2 = [2000, 'String Value 2000', -107.04]
            data = [row1, row2]
            client.insert('new_table', data, column_names=['key', 'value', 'metric'])
            print("Data Inserted")
            client.query('DROP TABLE new_table')
        except Exception as e:
            print("Insert failed")
            raise
    except Exception as e:
        print("Client failed to initialise")
        raise 
