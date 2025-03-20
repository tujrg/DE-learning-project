import pandas as pd
from sqlalchemy import create_engine

class LmsTraining:
    """LmsTraining class to handle the training result
    """

    def __init__(self):
        """Init LmsTraining required arguments
        Arguments:
            host: Host of the Postgres database (internal IP of Postgres VM Instance)
            database: Database name of the LMS table
            user: Username of the database
            password: Password that is required to connect to the database
            port: Opened port of LMS database
        """
        self.host = 'postgredb'
        self.database = 'streaming_data'
        self.user = 'postgres'
        self.password = 'postgres'
        self.port = '5432'  # Replace with your specific port number
        # Init we will assign values to it later after the training process is finished
        self.connection_string = ""
        self.engine = None

    def connect_to_DB(self):
        """
            Use to connect to DB postgres SQL
            Args:
                NONE
            Returns:
                connection engine:
                    sqlalchemy.engine.base.Engine
        """
        print("connecting to db ...")
        self.connection_string = f"host={self.host} dbname={self.database} user={self.user} password={self.password} port={self.port}"
        self.engine = create_engine(f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')
        print("connected to db ...")
        return self.engine
    
    def close_connection_to_db(self):
        """Close connection(if open) to Postgres database"""
        if self.engine == None:
            print(f'Connection to {self.host}.{self.database} is already closed!')
        else:
            self.engine.dispose()
            self.engine = None
            print('Connection to {self.host}.{self.database} is closed!')
    
    def insert_to_pg(self, df,  table_name, if_exists='append', index=False):
        """
            Args:
                df: dataframe use to upload data to postgres
                table_name: table name on postgres
                if_exists: if table on postgres exist, we will ('append','replace')
                index: bool, default True
                    Write DataFrame index as a column. Uses index_label as the column name in the table. Creates a table index for this column.
            Returns:
                Result: data frame be uploaded to table on postgres
        """
        print("Start to upload data to postgres data ...")
        with self.engine.connect() as connection:
            df.to_sql(name=table_name,con=connection, if_exists=if_exists, index=index)
            print(f"Finished to upload {df.shape[0]} records to {table_name} postgres data ...")

    def get_data_pg(self, query):
        """Get existing data on Postgres table.
            Args:
                query: the query use to query on DB
            Returns:
                Data Frame: contains data when we execute query
        """
        df = pd.read_sql_query(query, self.engine)

        return df