import logging
import pandas as pd

from .. import repository, model


class DataSyncService:

    def __init__(self):
        self.pg_db = repository.Postgres()
        # self.data_lake = repository.DataLake()

    # def sync_to_data_lake(self, data):
    #     if data["districts"] is None:
    #         df = self.data_lake.query(data['start_time'])
    #     else:
    #         df = self.data_lake.query(data['start_time'], data['districts'])
    #
    #     # TODO: Save the dataframe into the database
    #     # 1. Make sure to override data if needed
    #     # 2. Do not allow the corruption of data
