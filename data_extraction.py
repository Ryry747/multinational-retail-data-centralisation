import pandas as pd
from sqlalchemy import create_engine
from database_utils import DatabaseConnector

class DataExtractor:
    
    def __init__(self, db_connector):
        
        self.db_connector = db_connector
        
    def read_rds_table(self, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con=self.db_connector.db_engine)
        return df
