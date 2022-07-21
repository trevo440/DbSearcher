# DbSearcher
DbSearcher - The class for searching Through Databases
        
        Search Types:
        
                ->Table (info on a table level- row counts, table types, etc.)
                ->Column (info on a column level- data types, column size, etc.)
                ->Search (Search for a value inside of the database)
                ->MST (creates a massive table composed of all distinct values of all columns in the database...
                       Searching on this table is significantly faster)
                             
Possible inputs:

        Connection Info:
            conn_string (required) - ODBC connection string, uses pyodbc library for connection
            conn_type (required) - for ensuring sql queries will work against the data source (SQL, OTH)
            db_name  (required) - The name of the Database (dbo, etc. for SQLServer)
            
        Setup:
            search_type (required) - Table, Column, Search, MST (4 options, default: None)
        
        Filters:
            max_row_count (default None) - only applies to Search or MST
            data_type (default all types) - only applies to Search or MST. Possible types include: Bit, Blob, DateTime, Date, Time, Decimal, Float, Integer, String
            min_col_size (default None) - minimum internal size of column
            max_col_size (default None) - maximum internal size of column 
            table_list (default all tables) - provide a list of tables to limit searches
            column_list (default all columns) - provide a list of columns to limit searches
            
        Search Parameters:
            search_val (required for search_type=Search) - string to search. sql special characters work! %% *
            and_column (optional, default none) - Search including a statement AND column LIKE
            like_val (optional, default none) - paired with and_column  
            
After configuring paramters, call DbSearcher.search()
            -> returns a pd.DataFrame object of the output
