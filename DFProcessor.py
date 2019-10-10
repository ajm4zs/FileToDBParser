# Authors: Alex Mulchandani and Nick Kharas

import pyodbc
import urllib
import datetime
import string
import uuid
from sqlalchemy.engine import *
import sqlalchemy

from file_integrity_checks import checkFileExistence
from df_integrity_checks import isValueList, checkForLists, removeListColumnsFromDataframe
from JSONProcessor import JSONProcessor



class DFProcessor:

    def __init__(self, server='', database='', table_name='', drop_table_if_exists=1):
        self.server = server
        self.database = database
        self.table_name = table_name
        self.drop_table_if_exists = drop_table_if_exists

    # Generates the sql statement for creating a table from a dataframe
    def generate_create_table_sql(self, df):
        sql = 'CREATE TABLE dbo.[' + self.table_name + '] ('

        i = 1

        while i <= len(df.columns):
            if i == len(df.columns):
                sql += '[' + df.columns[i - 1] + '] NVARCHAR(2000))'
            else:
                sql += '[' + df.columns[i - 1] + '] NVARCHAR(2000),'

            i += 1

        return sql

    def generate_drop_table_sql(self):
        sql = str('DROP TABLE IF EXISTS dbo.[%s]'%self.table_name)

        return sql

    # Checks if table already exists and throws exception if exists
    def check_table_existence(self, connection):
        sql = "select * from sys.tables where name = '" + self.table_name + "'"

        result = connection.execute(sql)

        row = result.fetchone()

        return row

    # Creates sql table on db
    def create_sql_table(self, connection, df):

        # check existence of SQL table
        row = self.check_table_existence(connection)

        if (row and not self.drop_table_if_exists):
            return
        else:
                
            # compile SQL drop table statement
            drop_sql = self.generate_drop_table_sql()

            # compile SQL create table statement
            create_sql = self.generate_create_table_sql(df)

            try:
                connection.execute(drop_sql)
                connection.execute(create_sql)
            except:
                raise Exception('There was an issue dropping and creating the SQL table.')

        return

    # Gets engine from pyodbc and returns engine object
    def get_engine(self):

        try:
            connectionString = urllib.parse.quote_plus(
                'Driver={SQL Server Native Client 11.0};SERVER=' + self.server + ';DATABASE=' + self.database + ';Trusted_Connection=yes')
            engine = create_engine(
                'mssql+pyodbc:///?odbc_connect=%s' % connectionString)
        except:
            raise Exception('Unable to attain engine for database ' +
                            self.database + ' on server ' + self.server)
        finally:
            return engine

    # Connects engine and returns the connection
    def connect_engine(self, engine):

        try:
            connection = engine.connect()
        except:
            raise Exception('Cannot connect engine.')

        return connection

    # Writes the contents of a pandas dataframe to a sql table
    def write_dataframe_to_sql_table(self, df, engine, connection):

        # create the SQL table
        self.create_sql_table(connection, df)

        # write dataframe rows to SQL table
        try:
            df.to_sql(name=self.table_name, con=engine, index=False, if_exists='append')
            # data_normalized.to_sql(name=tableName, con=engine, index=False, dtype={col_name: sqlalchemy.types.NVARCHAR(length=2000) for col_name in data_normalized})
        except:
            raise Exception('There was an error writing the dataframe to SQL Server.')

        return

    # write JSON to output file
    def write_dataframe_to_output_file(self, df, outputPath, outputFileName, rowDelimeter):

        # store entire file path and name in fullOutputPath
        fullOutputPath = outputPath + '\\' + outputFileName + '.csv'

        # check for existence of output file
        checkFileExistence(fullOutputPath)

        df.to_csv(fullOutputPath, index=None, header=True, sep=rowDelimeter)

    def write_final_data(self, df, engine, connection):
        if (connection == 'file'):
            # Write df to output file in same directory as source file
            outputTableName = self.table_name + '_' + str(uuid.uuid4())
            self.write_dataframe_to_output_file(df, engine, outputTableName, '|')
        else:
            # Write df to SQL
            self.write_dataframe_to_sql_table(df, engine, connection)

    # process a dataframe by writing it's contents to sql table(s)
    def process_dataframe(self, df, engine, connection):

        # find if any columns contain a list
        listColumns = checkForLists(df)

        if len(listColumns) == 0:
            self.write_final_data(df, engine, connection)

        else:

            list_dict = {}

            for col in listColumns:
                # add new GUID (col_GUID) to df
                GUID_col = col + '_GUID'
                df[GUID_col] = [uuid.uuid4() for _ in range(len(df.index))]
                list_dict[col] = []

            # iterate through each row in df
            for index, row in df.iterrows():
                for col in listColumns:
                    GUID_col = col + '_GUID'
                    list_col = row[col]
                    list_col_GUID = row[GUID_col]

                    if (isValueList(list_col)):
                        for item in list_col:
                            if(isinstance(item, str)):
                                print(row)
                                print(list_col)
                                print(item)
                            item[GUID_col] = list_col_GUID
                            list_dict[col].append(item)

            # we now have the new dfs... let's get rid of list columns from original DF and write it to SQL
            dfWithoutListColumns = removeListColumnsFromDataframe(df, listColumns)

            self.write_final_data(dfWithoutListColumns, engine, connection)

            # now that new dfs are fully created, do something with them

            for col in listColumns:
                json_processor = JSONProcessor()
                new_df_normalized = json_processor.get_dataframe_from_json(list_dict[col])

                self.table_name = self.table_name + '_' + col
                self.process_dataframe(new_df_normalized, engine, connection)
