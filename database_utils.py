import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    
    def __init__(self, creds_path):
        self.creds_path = creds_path
        self.db_engine = self.init_db_engine()
        
    def read_db_creds(self):
        with open(self.creds_path, 'r') as f:
            creds = yaml.safe_load(f)
        return creds
    
    def init_db_engine(self):
        creds = self.read_db_creds()
        db_uri = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(db_uri)
        return engine
    
    def list_db_tables(self):
        return self.db_engine.table_names()

    def upload_to_db(self, df, table_name):
        df.to_sql(table_name, con=self.db_engine, if_exists='replace', index=False)