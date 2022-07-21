#IMPORT
import pyodbc
import struct
import pandas as pd

#message handling
def display_msg(func):
        def wrapper(*args, **kwargs):
            msg = func(*args, **kwargs)
            if msg != None:
                print(f'{msg[0]}{" "*(10-len(msg[0]))}-| {msg[1]}')    
        return wrapper

#DbSearcher Class
class DbSearcher():
    '''
    DbSearcher - The class for searching Through Databases
        Search Types: ->Table (info on a table level- row counts, table types, etc.)
                      ->Column (info on a column level- data types, column size, etc.)
                      ->Search (Search for a value inside of the database)
                      ->MST (creates a massive table composed of all distinct values of all columns in the database...
                             Searching on this table is significantly faster)
    '''
    
    def __init__(self, conn_string=None, conn_type=None, db_name=None, search_type=None, 
                 max_row_count=None, data_type=None, min_col_size=None, max_col_size=None, 
                 table_list=None,column_list=None,search_val=None, and_column=None, like_val=None):
        
        '''
        Possible inputs
            conn_string (required) - ODBC connection string, uses pyodbc library for connection
            conn_type (requied) - for ensuring sql queries will work against the data source (SQL, OTH)
            db_name  (required) - The name of the Database (dbo, etc. for SQLServer)
            search_type (required) - Table, Column, Search, MST (4 options, default: None)
            max_row_count (default None) - only applies to Search or MST
            data_type (default all types) - only applies to Search or MST. Possible types include: Bit, Blob, DateTime, Date, Time, Decimal, Float, Integer, String
            min_col_size (default None) - minimum internal size of column
            max_col_size (default None) - maximum internal size of column 
            table_list (default all tables) - provide a list of tables to limit searches
            column_list (default all columns) - provide a list of columns to limit searches
            search_val (required for search_type=Search) - string to search. sql special characters work! %% *
            and_column (optional, default none) - Search including a statement AND column LIKE
            like_val (optional, default none) - paired with and_column            
        '''
        #------------------------------------------------------------------------
        #USER-CONTROLLED PARAMETERS
        #------------------------------------------------------------------------
        
        #connection info
        self.__conn_string : str = conn_string              #conn_string
        self.__conn_type : str = conn_type
        self.__db_name : str =  db_name                     #db_name
        
        #search type - Schema (Column, Table) || Search (Value, MST)
        self.__search_type : str = search_type              #search_type
        
        #Filters - Search (Value, MST)
        self.__max_row_count : int = max_row_count          #max_row_count
        self.__data_type : str = data_type                  #data_type
        self.__translate = { "":"",
                          "Bit":"<class 'bool'>",
                          "Blob":"<class 'bytearray'>",
                          "DateTime":"<class 'datetime.datetime'>",
                          "Date":"<class 'datetime.date'>",
                          "Time":"<class 'datetime.time'>",
                          "Decimal":"<class 'decimal.Decimal'>",
                          "Float":"<class 'float'>",
                          "Integer":"<class 'int'>",
                          "String":"<class 'str'>" }
        
        
        
        self.__min_col_size : int = min_col_size            #min_col_size
        self.__max_col_size : int = max_col_size            #max_col_size
        
        self.__table_list : list = table_list               #table_list
        self.__column_list : list = column_list             #column_list
        
        #Search Params - Search (Value)
        self.__search_val : str = search_val                #search_val
        self.__and_column : str = and_column                #and_column
        self.__like_val : str = like_val                    #like_val

        self.config = {'conn_string':self.__conn_string,
                       'conn_type':self.__conn_type,
                       'db_name':self.__db_name,
                       'search_type':self.__search_type, 
                       'max_row_count':self.__max_row_count,
                       'data_type':self.__data_type, 
                       'min_col_size':self.__min_col_size,
                       'max_col_size':self.__max_col_size, 
                       'table_list':self.__table_list,
                       'column_list':self.__column_list, 
                       'search_val':self.__search_val,
                       'and_column':self.__and_column, 
                       'like_val':self.__like_val}          #config
        
        #------------------------------------------------------------------------
        #INTERNAL LOGIC
        #------------------------------------------------------------------------
        self.__df_out = []                                  #df_out
        self.__data_array = []                              #data_array
        
        self.__is_valid = False                             #is_valid
        self.__cursor = None                                #cursor  
        
        self.__internal_table_list = []                     #internal_table_list
        self.__internal_table_data = []                     #internal_table_data
        self.__internal_reference = {}                      #internal_reference
        self.__table_row_count = {}                         #internal_row_count
        
        self.__tableCount = 0                               #table_count
        self.__parseValue = 0                               #parse_value    
        
    @display_msg
    def configure(self, index, replacement):
        '''
        update parameters, can also use DbSearcher.config["parameter"]
        '''
        #update valuess in config dictionary
        if not isinstance('index', str) or index not in self.config.keys():
            return ['Error', 'Please Provide a Valid Parameter'] 
        self.config[index] = replacement
        self.__internal_validation()
        
    @display_msg
    def __internal_validation(self):
        '''
        validate parameters
        '''
        #conn_string
        if not self.config['conn_string']:
            return ['Error', 'Connection String Must Not Be Empty']
        if not isinstance(self.config['conn_string'], str):
            return ['Error', 'Connection String must be a String']
        
        #conn_type
        if not self.config['conn_type']:
            return ['Error', 'Connection Type Must Not Be Empty // Options: (SQL, OTH)']
        if not isinstance(self.config['conn_string'], str):
            return ['Error', 'Connection Type must be a String // Options: (SQL, OTH)']
        if self.config['conn_type'] not in ['SQL', 'OTH']:
            return ['Error', 'Connection Type Must be one of the Options // Options: (SQL, OTH)']
        
        #db_name
        if not self.config['db_name']:
            return ['Error', 'Database Name Must Not Be Empty']
        if not isinstance(self.config['db_name'], str):
            return ['Error', 'Database Name must be a String']
        
        #search_type
        if not self.config['search_type']:
            return ['Error', 'Search Type must not be empty // Options: (Column, Table, Search, MST)']
        if not isinstance(self.config['search_type'], str):
            return ['Error', 'Search Type must be a String // Options: (Column, Table, Search, MST)']
        
        #max_row_count
        if self.config['max_row_count']:
            if not isinstance(self.config['max_row_count'], int):
                return ['Error', 'Max Row Count must be an Integer']
            if self.config['max_row_count'] <= 0:
                return ['Error', 'Max Row Count must be greater than 0']
        
        #data_type
        if self.config['data_type']:
            if not isinstance(self.config['data_type'], str):
                return ['Error', 'Data Type must be String']
            if self.config['data_type'] not in self.__translate.keys():
                return ['Error', f'Data Type Must be One of {self.__translate.keys()}']
        
        #min_col_size
        if self.config['min_col_size']:
            if not isinstance(self.config['min_col_size'], int):
                return ['Error', 'Min Column Size must be an Integer']
            if self.config['min_col_size'] <= 0:
                return ['Error', 'Min Column Size must be greater than 0']
            
        #max_col_size
        if self.config['max_col_size']:
            if not isinstance(self.config['max_col_size'], int):
                return ['Error', 'Max Column Size must be an Integer']
            if self.config['max_col_size'] <= 0:
                return ['Error', 'Max Column Size must be greater than 0']
        
        #self.__internal_table_list
        if self.config['table_list']:
            if not isinstance(self.config['self.__internal_table_list'], list):
                return ['Error', 'Table List Must be a List']
        
        #column_list
        if self.config['column_list']:
            if not isinstance(self.config['column_list'], list):
                return ['Error', 'Column List Must be a List']
        
        #search_val
        if self.config['search_type'] == 'Search':
            if not self.config['search_val']:
                return ['Error', 'Search Value must not be empty for this Search Type']
            if not isinstance(self.config['search_val'], str):
                return ['Error', 'Search Value must be a String']
            
            #and_column
            if self.config['and_column']:
                if not isinstance(['and_column'], str):
                    return ['Error', 'And Column must be a String']
                if not self.config['like_val']:
                    return ['Error', 'Both And Column AND Like Value are required (or both None)']
            
            #like_val
            if self.config['like_val']:
                if not isinstance(self.config['like_val'], str):
                    return ['Error', 'Like Val must be a String']
                if self.config['and_column']:
                    return ['Error', 'Both And Column AND Like Value are required (or both None)']    
        
        #Internal Validation = PASS
        else:
            self.__is_valid = True
            return ['Info', 'Internal Validation = PASS']
               
    def __test_connection(self):    
        '''
        tests the odbc connection is setup correctly and puts in an output converter for SQL Server datetimeoffsets
        '''    
        try: 
            cxn = pyodbc.connect(self.config["conn_string"])
            cxn.add_output_converter(-155, self.handle_datetimeoffset)
            self.__cursor = cxn.cursor()
            self.display_text_msg(f'Connection Passed with {self.config["conn_string"]}')
            return True
        except:
            self.display_error_msg(f'Connection Failed with {self.config["conn_string"]}')
            return False
    
    def __pull_tables(self):
        '''
        initial meta-info grab of all tables in the database
        '''
        try:
            for table in self.__cursor.tables(schema=f'%{self.__db_name}%'):
                self.__internal_table_list.append(table.table_name)
                if self.config['search_type'] == 'Table':
                    self.__internal_table_data.append([table.table_name, table.table_schem, table.table_cat, table.table_type])
                if self.config['and_column'] is not None and self.config['like_val'] is not None:
                    self.__internal_reference[table.table_name] = []
            #self.display_info_msg(f'Table found: {table.table_name}')
            self.display_text_msg(f'Num Tables: {len(self.__internal_table_list)}')
            self.__tableCount = len(self.__internal_table_list)
            return True
        except:
            self.display_error_msg('Database Unable to be Parsed For Table Info')
            return False
        
    def __db_Table(self):
        '''
        *specifically for search_type = Table
        '''        
        if self.config['search_type'] == 'Table':
            valueList=['Database', 'Schema', 'Table', 'Type', 'Row Count', 'SQL']
            for table_info in self.__internal_table_data:
                self.__parseValue += 1
                self.display_info_msg(f'Parsing Table {self.__parseValue}/{self.__tableCount} for metadata | ({table_info[0]})')
                #"point" to specific table for gathering information
                try:
                    tableSqlString = f'SELECT COUNT(*) FROM "{self.config["db_name"]}"."{table_info[0]}"'
                    self.__cursor.execute(tableSqlString)
                    x = self.__cursor.fetchval()
                    self.__data_array.append([str(table_info[2]), str(table_info[1]), str(table_info[0]), str(table_info[3]), str(x), str(tableSqlString)])
               
                except:
                    self.display_text_msg(f"Parse Failed for Table {table_info[0]}")
            self.__df_out = pd.DataFrame(self.__data_array, columns=valueList)
        
    def __row_Count(self):
        '''
        grabs row count if max_row_count parameter is updated, for use in filters
        '''
        self.__parseValue = 0
        if self.config['max_row_count'] is not None:
            if self.config['search_type'] == 'MST' or self.config['search_type'] == 'Search':
                for table_info in self.__internal_table_list:
                    self.__parseValue += 1
                    self.display_info_msg(f'Parsing Table {self.__parseValue}/{self.__tableCount} for Row Count | ({table_info})')
                    #"point" to specific table for gathering information
                    try:
                        tableSqlString = f'SELECT COUNT(*) FROM "{self.__db_name}"."{table_info}"'
                        self.__cursor.execute(tableSqlString)
                        x = self.__cursor.fetchval()
                        self.__table_row_count[table_info] = int(x)
                    except:
                        self.display_text_msg(f"Parse Failed for Table {table_info[0]}")
        
    
    def __sub_run(self):
        '''
        The ~magic~
            -> aka I wrote this a few months ago and the logic is a lot to break down in a short summary
        '''
        sqlStringList = []
        for table in self.__internal_table_list:
            self.__parseValue += 1
            
            try:
                rowval = self.__table_row_count[table]
            except:
                rowval = 0
                
            self.display_info_msg(f'Parsing Table {self.__parseValue}/{self.__tableCount} for metadata | ({table})')
            
            try:
                #"point" to specific table for gathering information
                if self.config['search_type'] == 'SQL':
                    tableSqlString = f'SELECT TOP 1 * FROM "{self.config["db_name"]}"."{table}"'
                else:
                    tableSqlString = f'SELECT * FROM "{self.config["db_name"]}"."{table}" LIMIT 1'
                
                self.__cursor.execute(tableSqlString)
                
                if self.__internal_reference:
                    for row in self.__cursor.description:
                        self.__internal_reference[table].append(row[0])
                
                for row in self.__cursor.description:
                    
                    sortSignal = 0
                    #Logic gates for Search Params
                    if self.config['search_type'] == 'Search' or self.config['search_type'] == 'MST':
                        if self.config['data_type'] is not None and self.__translate[self.config['data_type']] != str(row[1]):
                            sortSignal += 1
                        elif self.config['max_row_count'] is not None and self.config['max_row_count'] < int(rowval):
                            sortSignal += 1   
                        elif self.config["min_col_size"] is not None and self.config["min_col_size"] > int(row[3]):
                            sortSignal += 1   
                        elif self.config["max_col_size"] is not None and self.config["max_col_size"] < int(row[3]):
                            sortSignal += 1
                        elif self.config["table_list"] is not None and table not in self.config["table_list"]:
                            sortSignal += 1
                        elif self.config["column_list"] is not None and row[0] not in self.config["column_list"]:
                            sortSignal += 1
                        elif self.config["and_column"] is not None:
                            if self.config["and_column"] not in self.__internal_reference[table] or self.config["and_column"] == row[0]:
                                sortSignal += 1
                    
                    #add sqlString to execute in main run
                    if sortSignal == 0 and self.__search_type != 'MST':
                        #construct SQL statement
                        if self.config['conn_type'] == 'SQL':
                            if self.__internal_reference:
                                sqlString = f'SELECT TOP 1 * FROM {self.config["db_name"]}.{table} WHERE UPPER([{self.config["db_name"]}].[{table}].[{row[0]}]) ' + f"LIKE UPPER('{self.config['search_val']}')" + f"AND [{self.config['db_name']}].[{table}].[{self.config['and_column']}] LIKE '{self.config['like_val']}'"
                            else:
                                sqlString = f'SELECT TOP 1 * FROM {self.config["db_name"]}.{table} WHERE UPPER([{self.config["db_name"]}].[{table}].[{row[0]}]) ' + f"LIKE UPPER('{self.config['search_val']}')"
                        else:
                            if self.__internal_reference:
                                sqlString = f'SELECT * FROM {self.config["db_name"]}.{table} WHERE UPPER({self.config["db_name"]}.{table}.{row[0]}) ' + f"LIKE UPPER('{self.config['search_val']}')" + f"AND {self.config['db_name']}.{table}.{self.config['and_column']} LIKE '{self.config['like_val']}' LIMIT 1"
                            else:
                                sqlString = f'SELECT * FROM {self.config["db_name"]}.{table} WHERE UPPER({self.config["db_name"]}.{table}.{row[0]}) ' + f"LIKE UPPER('{self.config['search_val']}') LIMIT 1"
                        self.sqlStringList.append([sqlString, table, row[0], row[1], row[2], row[3]])        
                    
                    if sortSignal == 0 and self.__search_type == 'MST':
                        if self.config['conn_type'] == 'SQL':
                            sqlString = f'SELECT [{self.config["db_name"]}].[{table}].[{row[0]}], COUNT([{self.config["db_name"]}].[{table}].[{row[0]}]) FROM [{self.config["db_name"]}].[{table}] GROUP BY [{self.config["db_name"]}].[{table}].[{row[0]}]'            
                        else:
                            sqlString = f'SELECT {self.config["db_name"]}.{table}.{row[0]}, COUNT({self.config["db_name"]}.{table}.{row[0]}) FROM {self.config["db_name"]}.{table} GROUP BY {self.config["db_name"]}.{table}.{row[0]}'       
                        sqlStringList.append([sqlString, table, row[0], row[1], row[2], row[3]])
            except:
                #shows views, etc.
                self.display_text_msg(f"table {table} not data-related")
            
        #if meta info        
        if sqlStringList:
            self.__progressVar = 0
            totalCount = len(sqlStringList)
            for item in sqlStringList: #limit for testing
                self.__progressVar += 1
                #config settings for searching by value
                if self.config['search_type'] == 'Search':
                    valueList=['Table', 'Column', 'Type', 'Display Size', 'Internal Size', 'SQL', 'Output Sample']
                    self.display_info_msg(f'Executing Search-Query ({self.__progressVar}/{totalCount}) | {int((self.__progressVar/totalCount) * 100)}%')
                    try:
                        self.__cursor.execute(item[0])
                        x = self.__cursor.fetchone()
                        if x:
                            #contructing row to output
                            self.display_text_msg(str(x))
                            self.__data_array.append([str(item[1]), str(item[2]), str(item[3]), str(item[4]), str(item[5]), str(item[0]), str(x)])   #CHANGE TO DF OUTPUT
                    except:
                        pass  
                         
                #config settings for schema        
                if self.config['search_type'] == 'Column':
                    valueList=['Table', 'Column', 'Type', 'Display Size', 'Internal Size']
                    self.__data_array.append([str(item[1]), str(item[2]), str(item[3]), str(item[4]), str(item[5])]) #CHANGE TO DF OUTPUT
                    
                if self.config['search_type'] == 'MST':
                    valueList=['Table', 'Column', 'Type', 'Value', 'Count']
                    self.display_info_msg(f'Executing Distinct Value Fetch | ({self.__progressVar}/{totalCount}) | {int((self.__progressVar/totalCount) * 100)}%')
                    try:
                        self.__cursor.execute(item[0])
                        x = self.__cursor.fetchall()
                        if x is not None:
                            for values in x:
                                self.__data_array.append([str(item[1]), str(item[2]), str(item[3]), str(values[0]), str(values[1])]) #CHANGE TO DF OUTPUT
                    except:
                        pass
            self.__df_out = pd.DataFrame(self.__data_array, columns=valueList)
    
    def __clean_internal(self):
        '''
        reset prior to each run
        '''
        self.__internal_table_list = []
        self.__internal_table_data = []
        
        self.__data_array = []
        self.__df_out = []
        
        self.__internal_reference = {}
        self.__table_row_count = {}
        self.__tableCount = 0                               
        self.__parseValue = 0
    
    def search(self):
        '''
        After configuring paramters, call DbSearcher.search()
            -> returns a pd.DataFrame object of the output
        '''
        self.__is_valid = False
        self.__clean_internal() 
        self.__internal_validation()
        
        if self.__is_valid:
            passConnection = self.__test_connection()
        if passConnection:
            passTable = self.__pull_tables()
            
        if passTable:
            if self.config['search_type'] == 'Table':
                self.__db_Table()
            else:
                self.__row_Count()
                self.__sub_run()
        
        return self.__df_out
    
    #-------------------------------------------------------------------------------------------------------------------
    #Output Logs
    #-------------------------------------------------------------------------------------------------------------------    
    @display_msg
    def display_error_msg(self, msg_string: str):
        return ['Error', msg_string]

    @display_msg          
    def display_text_msg(self, msg_string: str):
        return ['Log', msg_string]

    @display_msg
    def display_info_msg(self, msg_string: str):
        return ['Info', msg_string]
           
    def handle_datetimeoffset(self, dto_value):
        tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 0, -6, 0)
        tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]
        return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}".format(*tweaked)
    
