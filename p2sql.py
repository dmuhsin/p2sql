import pyodbc
import pandas
import numpy as np

def connection_string(Server: str, Database = 'master', Driver = '{SQL Server}',  IntegratedSecurity = True, user=None, passw=None ):
    '''conection_string() stores the conection information, the only parameters that are required are the Server, ODBC Driver
    *Note: It is recommended to supply the databasename, by default "master" will be used* '''
    if IntegratedSecurity:
        connection_string = """
            Driver={driver};
            Server={server};
            Database={database};
            Trusted_Connection=yes;""".format(driver=Driver, server=Server, database=Database)
    else:
        connection_string = """
            Driver={driver};
            Server={server};
            Database={database};
            UID={user};
            PWD={passw};""".format(driver=Driver, server=Server, database=Database, user=user, passw=passw)
    return connection_string
def connection(connection_string):
    '''use connection() to establish a connectin to server. Here we use the above created connection string as the connection string parameter.'''
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursorDict = {"conn":conn,"cursor":cursor}
    return cursorDict

def getListOfDbs(connection):
    '''Use getListOfDbs() to get the list of database, by passing the connection function to getListOfDBs'''
    querystring = """
        SELECT name, database_id
        FROM master.sys.databases;  
        
    """
    try:
        getDB = connection['cursor'].execute(querystring)
        listOfDBs = getDB.fetchall()
    except:
        listOfDBs = "error: COULD NOT GET LIST OF DBs"

    return listOfDBs
def putInTable(connection,tablename: str,listOfColumnNames: list, listOfListValues: list ,Database="" ,schema='dbo',printQuery = False):
    '''putInTable() will create and run an insert execute query to the specified table, This function does not return a value, however the printQuery can be set to True to view the query that is inserted'''
    colNamesSQL = ""
    for colName in listOfColumnNames:
        colNamesSQL = colNamesSQL +  f""" {colName},""" 
    colNamesSQL = colNamesSQL.strip(",")
    rowValuesSQL = "" 
    insertQuery = ""
    i=0
    if Database !="":
        Database = f"[{Database}]."

    for rows in listOfListValues:
        rowValuesSQL = "" 
        if len(rows) == len(listOfColumnNames):
            for value in rows:
                rowValuesSQL = rowValuesSQL +  f""" '{value}',"""
            rowValuesSQL = rowValuesSQL.strip(",")     
            insertQuery = insertQuery +  f""" INSERT INTO {Database}[{schema}].[{tablename}]({colNamesSQL}) VALUES({rowValuesSQL});\n"""
        else:
            print(f'length of values in in column list ({len(listOfColumnNames)}) do not match the length of values in list values ({len(rows)})')
    if printQuery == True:
     print(insertQuery.replace("'Null'","Null"))

    try:
        connection['cursor'].execute(insertQuery)
    except:
        print('There was an error running query on server, check query by setting printQuery parameter to True')
    try:
        connection['conn'].commit()
    except:
        connection['conn'].close()
        print('There was an error running query on server, check query by setting printQuery parameter to True connection has closed')
def getFromTable(connection, tablename: str,Database="", printQuery = False,listOfColumnNames = ['*'] , whereClause=[[]],schema='dbo'):
    '''gertFromTable() will run a select query against the conneciotn and  return a list of tuples from the specified table 
      
    Example:   [('Daivd', 45, 'Detroit'), ('Mike', 45, 'Null')]'''
    colNamesSQL = ""
    whereString = ""
    for colName in listOfColumnNames:
        colNamesSQL = colNamesSQL +  f""" {colName},""" 
    colNamesSQL = colNamesSQL.strip(",")
    if Database !="":
        Database = f"[{Database}]."

    if whereClause != [[]]:
        for conditions in whereClause:
            whereString = whereString +  f""" {conditions[0]} = '{conditions[1]}' and""" 
        whereString = whereString.strip("and")
        insertQuery = f""" select {colNamesSQL} from {Database}[{schema}].[{tablename}] where {whereString} ;\n"""   
    else:
        insertQuery = f""" select {colNamesSQL} from {Database}[{schema}].[{tablename}];\n"""
      

    if printQuery:
        print(insertQuery)
    
    try:
        connection['cursor'].execute(insertQuery)
    except:
        print('There was an error running query on server, check query by setting printQuery parameter to True')
        connection['conn'].close()

    return connection['cursor'].fetchall()
def createTblQuery(database:str,tablename:str,columns:list,dataTypes=False,schema='dbo'):
    '''createTblQuery() requires a database name, a table name, and a list of lists,  example: *[['Column Name','Data Type']]*   where index 0 is the name of the column and index 1 is the SQL data type '''
    createColumnStrQuery = ""
    for columnName in columns:
        createColumnStrQuery = createColumnStrQuery + f" {columnName[0]} NVARCHAR(MAX) NULL ,"
    createColumnStrQuery = createColumnStrQuery[:-1]

    createQuery =f"""

        IF OBJECT_ID('{tablename}') IS Null
        CREATE TABLE [{database}].[{schema}].[{tablename}]
        (
        {createColumnStrQuery}
        )
    
    """
    if dataTypes:
        try:
            createColumnStrQuery = ""
            i=0
            for columnName in columns:
                createColumnStrQuery = createColumnStrQuery + f" {columnName[0]} {columnName[1]} ,"
                i=i+1
            createColumnStrQuery = createColumnStrQuery[:-1]

            createQuery =f"""

                IF OBJECT_ID('{tablename}') IS Null
                CREATE TABLE [{database}].[{schema}].[{tablename}]
                (
                {createColumnStrQuery}
                )
            
            """        
        except:
            print("there was an error generating your create table query. check your data types")
    print("NOTE: In future versions you will have more flexibility to create and modify database tables ")
        
    return createQuery
def executeCreateDB(connection,listOfDBNames,printQuery=False):
    '''use executeCreateDB() to create database(s) by using the executeCreateDB function , passing the conneciton variable and a python list of databasename strings'''
    for dbName in listOfDBNames:
        querystring = f"CREATE DATABASE {dbName};"
        if printQuery == True:
            print(querystring)
        try:
            connection['conn'].autocommit = True
            connection['cursor'].execute(querystring)
        except:
            print("error: COULD NOT CREATE DATABASE")
def executeQueries(connection, listOfQueries,printQuery=False):
    '''executeQueries() executes a list of query strings such as insert,create,update,etc. This function does not return a value it loops through a list of supplied queries and executes the query using the module pyodbc connection '''
    for querystring in listOfQueries:
        if printQuery == True:
            print(querystring)
        try:
            connection['cursor'].execute(querystring)
        except:
            continue
        try:
            connection['conn'].commit()
        except:
            connection['conn'].close()

def merge(values: list,columns:list, targetTable: str, matchOnSource : list, matchOnTarget : list, WHEN_MATCHED_THEN_UPDATE_target = True, NOT_MATCHED_BY_SOURCE_DELETE = True, NOT_MATCHED_BY_SOURCE_query = "", printQuery = False  ):
    ''''''
    queryValues = ''
    colnames = ''
    matchOn = ''
    insertColSource = ''
    updateMatchOn = ''
    
    if NOT_MATCHED_BY_SOURCE_DELETE:
       NOT_MATCHED_BY_SOURCE_query =  "WHEN NOT MATCHED BY Source THEN DELETE;"

    for col in columns:
        colnames = colnames + f'[{col}],'
    colnames = colnames.rstrip(',')

 # convert Python data to Temp table 
    for row in values:
        fields=''
        for field in row:
            fields = fields + f"'{field}',"
        queryValues =queryValues + f"({fields.rstrip(',')}),"
    queryValues = queryValues.rstrip(',')

#   For match on
    i=0
    while i < len(matchOnSource):
        matchOn =  matchOn +  f" Source.[{matchOnSource[i]}] = Target.[{matchOnTarget[i]}] ,"
        i=i+1
    matchOn = matchOn.rstrip(',')

#   For Inserts

    for col in columns:
        insertColSource = insertColSource + f" Source.[{col}] ," 
    insertColSource = insertColSource.rstrip(',')

#   For Updates
    if WHEN_MATCHED_THEN_UPDATE_target:
        i=0
        while i < len(columns):
            updateMatchOn =  updateMatchOn +  f" Target.[{columns[i]}] = Source.[{columns[i]}] ,"
            i=i+1
        updateMatchOn = updateMatchOn.rstrip(',')
    else:
        i=0
        while i < len(columns):
            updateMatchOn =  updateMatchOn +  f" [{columns[i]}] = Target.[{columns[i]}] ,"
            i=i+1
        updateMatchOn = updateMatchOn.rstrip(',')



    querystring = f"""
    with DF_SQL_TBL_8675 as ( select * from(
    VALUES {queryValues}) tempTable ({colnames})
    )

    MERGE {targetTable} AS Target
    USING  DF_SQL_TBL_8675	AS Source
    ON {matchOn}
        
    -- For Inserts
    WHEN NOT MATCHED BY Target THEN
        INSERT ({colnames}) 
        VALUES ({insertColSource})
        
    -- For Updates
    WHEN MATCHED THEN UPDATE SET
    {updateMatchOn}
    -- For Deletes
    {NOT_MATCHED_BY_SOURCE_query}       

    """

    if printQuery == True:
        print(querystring)
    return querystring





## creates Pandas Dataframe to SQL
def df_createTblQuery(database: str,tablename: str, dataframe: pandas.DataFrame,printQuery=False,schema='dbo'):
    '''df_createTblQuery(), takes the column headers from a pandas DataFrame and creates a SQL create Table query.
    *Note: that this query will not insert data into the created table even if the DataFrame contains data for that you would need to use df_insertQuery(). All of the datatypes will be of type NVARCHAR(MAX)*
'''
    columnNames=""
    numOfColumns = len(dataframe.columns)
    for index,column in enumerate(dataframe):
        
        if index < numOfColumns-1:
            comma = ", \n"
        else:
            comma =" "
        columnNames = columnNames + f"""{column} NVARCHAR(MAX) NULL {comma}"""


    createQuery = f"""
    IF OBJECT_ID('{tablename}') IS Null
    CREATE TABLE [{database}].[{schema}].[{tablename}]
    (
    {columnNames}
    )

    """
    if printQuery == True:
        print(createQuery)
    return createQuery
def df_insertQuery(database: str,tablename: str, dataframe: pandas.DataFrame,printQuery=False,schema='dbo'):
    '''df_insertQuery() will itterate through pandas dataframe and insert data into a sql database table. 
    *Note that the DataFrame column namnes need to match the database column names.*'''
    columnNames=""
    value_columnNames=""
    insertQuery=""
    numOfColumns = len(dataframe.columns)
    for index,column in enumerate(dataframe):
        if index < numOfColumns-1:
            comma = ", "
        else:
            comma =" "
        columnNames = columnNames + f"""{column} {comma}"""
        value_columnNames = value_columnNames + f"?{comma}"
    for index, row in dataframe.replace(np.nan, 'Null', regex=True).iterrows():
            rowValues=','.join("'"+row.values+"'")
            insertQuery = insertQuery + f""" INSERT INTO [{database}].[{schema}].[{tablename}]({columnNames})VALUES({rowValues});\n""".replace("'Null'","Null")
    if printQuery == True:
        print(insertQuery)
    return insertQuery
def df_mergeQueryTables(database: str,targetTable: str,sourceTable: str, target_source_columns: list, dataframe: pandas.DataFrame, mergeDelete: bool, deleteStatement: str,printQuery=False,schema='dbo' ):
    '''df_mergeQueryTables() creates a merge query, merges data from source table to target table.  as long as the supplied dataframe matches the source and target table.

        WHEN target table ON target_source_columns  NOT MATCHED BY TARGET   
        then insert source table rows with insert query  
            
        WHEN source table column MATCHED target  
            then update target table with update query  
            
        WHEN NOT MATCHED BY SOURCE   
            THEN DELETE   
            
        Unless mergeDelete is set to True,  
            then set deleteStatement to whatever query you want to run when not matched by source   '''
    columnNames=""
    value_columnNames=""
    update_columnNames=""
    onTargetSource=""
    numOfColumns = len(dataframe.columns)
    for index,column in enumerate(dataframe):
        if index < numOfColumns-1:
            comma = ", "
        else:
            comma =" "
        columnNames = columnNames + f"""{column} {comma}"""
        value_columnNames = value_columnNames + f"Source.{column}{comma}"
        update_columnNames = update_columnNames + f"Target.{column}{comma} = Source.{column}{comma}"
        
    for index,each in enumerate(target_source_columns):
        if ((index+1) % 2) == 0:
            targetIndex = index-1
            sourceIndex = index
            if index < len(target_source_columns)-1:
                add_and = "and "
            else:
                add_and =" "
            onTargetSource = onTargetSource+f"Target.[{target_source_columns[targetIndex]}] = Source.[{target_source_columns[sourceIndex]}] {add_and}"
    if mergeDelete == True:
        mergeDeleteStatement = "WHEN NOT MATCHED BY SOURCE THEN DELETE "
    else:
        mergeDeleteStatement = f"""{deleteStatement}"""
    mergeQuery = f"""

        MERGE [{database}].[{schema}].[{targetTable}] AS Target

            USING [{database}].[{schema}].[{sourceTable}] AS Source
            on {onTargetSource}
            WHEN NOT MATCHED BY TARGET
            THEN 	
            INSERT ({columnNames})
            VALUES ({value_columnNames})
            WHEN MATCHED
            THEN UPDATE SET
            {update_columnNames}
            
            """ + mergeDeleteStatement

    if printQuery == True:
        print(mergeQuery)
    return mergeQuery  




# possible feature


# def df_insert_manyQuery(database: str,tablename: str, dataframe: pandas.DataFrame,printQuery=False,schema='dbo'):
#     columnNames=""
#     value_columnNames=""
#     numOfColumns = len(dataframe.columns)
#     for index,column in enumerate(dataframe):
#         if index < numOfColumns-1:
#             comma = ", "
#         else:
#             comma =" "
#         columnNames = columnNames + f"""{column} {comma} \n"""
#         value_columnNames = value_columnNames + f"?{comma}"


#     insertQuery = f"""

#     INSERT INTO [{database}].[{schema}].[{tablename}]
#     (
#     {columnNames}
#     )
#     VALUES
#     (
#     {value_columnNames}
#     )
#     """
#     if printQuery == True:
#         print(insertQuery)
#     return insertQuery
# def df_executeInsertQueryList(connection, listOfInsertQueries, listOfpandas_dataframe):
#     '''loops through a list of supplied queries and a list of pandas dataframes and executes accordingly'''
#     i = 0
#     for querystring in listOfInsertQueries:
#         print(listOfpandas_dataframe[i])
#         connection['cursor'].executemany(querystring, listOfpandas_dataframe[i])
#         i = i+1
#         try:
#             connection['conn'].commit()
#         except:
#             connection['conn'].close()


