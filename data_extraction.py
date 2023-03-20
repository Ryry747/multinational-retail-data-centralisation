import pandas as pd
import tabula
from data_cleaning import DataCleaning


class DataExtractor:
    
    def __init__(self, db_connector):
        
        self.db_connector = db_connector
        self.data_cleaner = DataCleaning()
        
    def read_rds_table(self, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, con=self.db_connector.db_engine)
        return df
    
    def clean_table(self, table_name):
        df = self.read_rds_table(table_name)
        cleaned_df = self.data_cleaner.clean_user_data(df)
        return cleaned_df
    
    def retrieve_pdf_data(self, link):
        # extract data from all pages of pdf and return as pandas dataframe
        df_list = tabula.read_pdf(link, pages='all', multiple_tables=True)
        df = pd.concat(df_list)
        return df

    