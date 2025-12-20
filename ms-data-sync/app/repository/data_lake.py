import pandas as pd

from databricks import sql


class DataLake:
    def __init__(self):
        self.connection = sql.connect(
            server_hostname="localhost",
            http_path="",
            access_token="",
        )

    def query(self, start_time, district=None):
        synchronization_query = ""
        with self.connection.cursor() as cursor:
            cursor.execute(synchronization_query)

            columns = [col[0] for col in cursor.description]

            return pd.DataFrame.from_records(cursor.fetchall(), columns=columns)
