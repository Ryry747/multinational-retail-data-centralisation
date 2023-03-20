import pandas as pd
from datetime import datetime
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class DataCleaning:
    
    def __init__(self, df):
        self.df = df
        
    def clean_user_data(self):
        # Drop rows with NULL values
        self.df.dropna(inplace=True)
        
        # Convert date column to a datetime format
        self.df['date_col'] = pd.to_datetime(self.df['date_col'], format='%Y-%m-%d')
        
        # Convert incorrect data types
        self.df['int_col'] = pd.to_numeric(self.df['int_col'], errors='coerce')
        self.df['float_col'] = pd.to_numeric(self.df['float_col'], errors='coerce')
        
        # Remove rows with incorrect data
        self.df = self.df[(self.df['int_col'] > 0) & (self.df['float_col'] > 0)]
        
        # Remove rows with invalid dates
        self.df = self.df[(self.df['date_col'] > datetime(2020, 1, 1)) & (self.df['date_col'] < datetime(2022, 1, 1))]
        
        # Reset index after cleaning
        self.df.reset_index(drop=True, inplace=True)
