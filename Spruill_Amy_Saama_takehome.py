# This class assumes the target database and table already exist

import pandas as pd
import json
from sqlalchemy import create_engine, MetaData, Table, select, update, bindparam

class DataReader:
    
    def __init__(self, csv_list, database_credentials):
        # csv_list: a list of string filepaths to the relevant .csv files
        # database_credentials: a dictionary of strings containing database information and credentials to be used
        #                       when connecting to the target database and table, structured as follows:
        #
        #                database_credentials = {
        #                    'username': 'mysql_account_username',
        #                    'password': 'mysql_account_password',
        #                    'host': 'target_host',
        #                    'database_name': 'target_MySQL_database_name',
        #                    'table_name': 'target_MySQL_table_name'
        #                }
        #
        self.csv_list = csv_list
        self.database_credentials = database_credentials
        
    def ReadAndUploadData(self):
        
        def read_database_table(query: str, con, index_cols: list): 
            # query: SQL query text
            # con: SQLAlchemy engine
            # index_cols: list of string names of columns to use as the primary key
            data_read = pd.read_sql_query(query,
                            con = engine,
                            index_col=index_cols,
                            chunksize = 2000)
            return pd.concat(data_read)


        def read_data_csv(csv_file_path: str, index_cols: list):  
            # csv_file_path: file path to csv, 
            # index_cols: list of string names of columns to use as the primary key
            data_read = pd.read_csv(csv_file_path, 
                            chunksize = 2000, 
                            dtype = str)
            return pd.concat(data_read).set_index(index_cols)


        def identify_insertions_and_deletions(curr_data, new_data):
            # curr_data: pandas dataframe of the current dataset (i.e. Field 1-5) for 
            #            comparison against new values
            # new_data: pandas dataframe of the new dataset
            # extracting primary keys for comparison
            curr_data_index = curr_data.index.tolist()
            new_data_index = new_data.index.tolist()
            # comparing primary keys for INSERTIONS
            insertions = [item not in curr_data_index for item in new_data_index]
            inserted_vals = new_data[insertions]
            inserted_vals['IUD'] = 'I'
            # comparing primary keys for DELETIONS
            deletions = [item not in new_data_index for item in curr_data_index]
            deleted_vals = curr_data[deletions]
            deleted_vals['IUD'] = 'D'
            return inserted_vals, deleted_vals


        def identify_updates(curr_data, new_data, inserted_vals_index):
            # curr_data: pandas dataframe of the current dataset (i.e. Field 1-5) for 
            #            comparison against new values
            # new_data: pandas dataframe of the new dataset
            # inserted_vals_index: pandas dataframe.index object derived from inserted_vals (the dataframe of new 
            #            rows to be inserted); used to filter new_data for more efficient comparison
            # dropping inserted values from new_data to avoid redundant comparisons
            new_data = new_data.drop(index = inserted_vals_index)
            # creating lists of values for efficient comparison of primary keys shared by current and new datasets
            curr_shared_index_vals = curr_data.values.tolist()
            new_shared_index_vals = new_data.values.tolist()
            updates = [item not in curr_shared_index_vals for item in new_shared_index_vals]
            updated_vals = new_data[updates]
            updated_vals['IUD'] = 'U'
            return updated_vals


        def update_database_values(updates_dataframe, engine, tablename): 
            # updates_dataframe: pandas dataframe of changed values (ie, deletions (IUD -> D) 
            #                    and updates (Fields 3-5 -> new values; IUD -> U))
            # engine: SQLAlchemy engine
            # tablename: string name of the target database table
            if updates_dataframe.empty:
                return
            else:
                # creating JSON list of values to for efficient database update
                updates_records = updates_dataframe.set_axis(['new_Field1', 
                                'new_Field2', 
                                'new_Field3', 
                                'new_Field4', 
                                'new_Field5',
                                'new_IUD'], axis='columns').to_json(orient='records')
                update_statement_values = json.loads(updates_records)
                # use SQLAlchemy Table object and statement to update database records
                metadata_obj = MetaData()
                target = Table(tablename, metadata_obj, autoload_with=engine)
                stmt = (
                update(target)
                .where(target.c.Field1 == bindparam('new_Field1'))
                .where(target.c.Field2 == bindparam('new_Field2'))
                .values(Field3=bindparam('new_Field3'), 
                        Field4=bindparam('new_Field4'), 
                        Field5=bindparam('new_Field5'), 
                        IUD=bindparam('new_IUD'))
                )
                with engine.begin() as conn:
                    conn.execute(
                        stmt,
                        update_statement_values,
                    )
            return print('Updated '+str(len(updates_dataframe))+' rows')


        def insert_database_values(inserted_vals_dataframe, engine, tablename): 
            # inserted_vals_dataframe: pandas dataframe of new rows to be inserted
            # engine: SQLAlchemy engine
            # tablename: string name of the target database table
            if inserted_vals.empty:
                return
            else:
                # use pandas dataframe.to_sql() as a convenient and efficient option to insert new rows
                inserted_vals.to_sql(tablename, 
                                            con=engine, 
                                            if_exists='append', 
                                            index=True, 
                                            chunksize=2000)
                return print('Inserted '+str(len(inserted_vals))+' new rows')


        def create_engine_string(self):
            # creates a valid engine string to pass to SQLALchemy create_engine() using the 
            # dictionary of database credentials passed to self
            return ''.join(['mysql+pymysql://',
                                 self.database_credentials['username'],
                                 ':',
                                 self.database_credentials['password'],
                                 '@',
                                 self.database_credentials['host'],
                                 '/',
                                 self.database_credentials['database_name']])
    
        # create SQLAlchemy engine
        engine_string = create_engine_string(self)
        engine = create_engine(engine_string)
        
        for csv_filepath in self.csv_list:
            # 1) load and process current dataset (from database table)
            query = 'select * from '+self.database_credentials['table_name']+' where IUD != "D"'
            current_dataset_with_IUD = read_database_table(query, engine, ['Field1', 'Field2'])
                # drop the IUD column to compare primary keys and values of Field 3-5 only
            curr_data = current_dataset_with_IUD.drop('IUD', axis=1)
            # 2) load and process new data (from .csv file)
            new_data = read_data_csv(csv_filepath, ['Field1', 'Field2'])
            # 3) identify inserted values, deleted values
            inserted_vals, deleted_vals = identify_insertions_and_deletions(curr_data, new_data)
            # 4) commit inserts to database
            insert_database_values(inserted_vals, engine, self.database_credentials['table_name'])
            # 5) identify updated values
            updated_vals = identify_updates(curr_data, new_data, inserted_vals.index)
                # create one dataframe containing deletions and updates; ie, all rows with existing primary 
                # keys that have changed values
            updates_dataframe = pd.concat([deleted_vals, updated_vals]).reset_index()
                # split updates_dataframe into chunks for more efficient database update
            update_chunk_size = 2000
            updates_df_list = [updates_dataframe[i:i+update_chunk_size] 
                               for i in range(0,len(updates_dataframe),update_chunk_size)]
            # 6) commit updates and deletions to database
            for update_df_chunk in updates_df_list:
                update_database_values(update_df_chunk, engine, self.database_credentials['table_name'])
            completion_message = ''.join(['Read, evaluated, and uploaded new data from ',
                                          csv_filepath,
                                          ' to table ',
                                          self.database_credentials['database_name'],
                                          '.',
                                          self.database_credentials['table_name']])
            print(completion_message)
        return

    
    
    
    
## to test with sample inputs:
dataset_list = ['first_load.csv', 'second_load.csv', 'third_load.csv', 'fourth_load.csv']

target_database_credentials = {
    'username': input_your_username,
    'password': input_your_password,
    'host': input_your_host,
    'database_name': input_your_target_database_name,
    'table_name': input_your_target_table_name
}

# If necessary or desired to secure the database connection with SSL, append the following additional key-value 
# pair to target_database_credentials:
#     'ssl_args': {'ssl': {'cert':'/path/to/client-cert', 
#                          'key':'/path/to/client-key', 
#                          'ca':'/path/to/ca-cert'}}
# Then adjust create_engine implementation as follows:
#     engine = create_engine(engine_string, connect_args=self.database_credentials['ssl_args'])


test = DataReader(dataset_list, target_database_credentials)
test.ReadAndUploadData()