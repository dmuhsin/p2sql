
# p2sql
## p2sql (Pandas and PYODBC To SQL) provides a simpler way to manage communications between python and SQL using dependencies such as: 
- pandas 
- pyodc
- and numpy


# Getting Started

```python
import p2sql
```

# Functions


## connection_string()
    connection_string(
        Server: str, 
        Database = 'master', 
        Driver = '{SQL Server}', 
        IntegratedSecurity = True, 
        user=None, 
        passw=None
    ) 


conection_string() stores the conection information, the only parameters that are required are the Server, ODBC Driver
    
<sub>Note: It is recommended to supply the databasename, by default "master" will be used 

```python
conn_str_master = p2sql.connection_string('servername')
```

The value of the variable conn_str_master will be:
   
```python
   Driver={SQL Server};  
   Server=servername;  
   Database=master;  
   Trusted_Connection=yes;  
```

You can create a different p2sql connection string by specifying the database or by setting intergrated security to false and setting the username and password parameters.  

Example:   

```python 
    conn_str_master = p2sql.connection_string (  
        'Server Name',  
        '{Enter Driver name}',  
        'Database Name',  
        False,   
        'UserName',  
        'Password'  
    )  
  ```  



## connection() 
    connection( conection_string() )

Use connection() to establish a connectin to server. Here we use the above created connection string as the connection string parameter.


    Note: You could write a custom connection string manually in the p2sql.connection() function 
</sub></sub>  

Example:   

    p2sql.connection('Driver={SQL Server}; Server=servername; Database=master; Trusted_Connection=true;') 

or
```python
connection = p2sql.connection(conn_str_master)
```

## executeCreateDB()

    executeCreateDB( connection(), listOfDBNames, printQuery=False  )
  
  
Use executeCreateDB() to create database(s) by using the executeCreateDB function , passing the conneciton variable and a python list of databasename strings

```python
p2sql.executeCreateDB(connection(),['TestDatabase','databaseTest2'],True)
```

## getListOfDbs( )
    getListOfDbs( connection() )
  
Use getListOfDbs() to get the list of database, by passing the connection function to getListOfDBs

```python
listOfDbs = p2sql.getListOfDbs(connection)
```

getListOfDbs() will result to a list of tuples containing (databaseName, databaseID)  
  
Example:  
```python 
[('master', 1), ('tempdb', 2), ('model', 3), ('msdb', 4), ('TestDatabase', 5), ('databaseTest2', 6)]
```

## createTblQuery()
    createTblQuery( database:str, tablename:str, columns:list, dataTypes=False, schema='dbo' )

createTblQuery() requires a database name, a table name, and a list of lists,  

Example: [['Column Name','Data Type']]   

where index 0 is the name of the column and index 1 is the SQL data type 


```python
columnsWithDataTypes = [
    ['name', 'nvarchar(255) NOT NULL' ],
    ['age', 'int NULL' ],
    ['location', 'nvarchar(100) NULL']
    ]
```

A list of columns without data types will default create a table with data types of 'nvarchar(MAX) NULL'  

Example:
```python
columnsWithoutDataTypes = [
['name'],
['age'],
['location']
]

TestTableQuery = p2sql.createTblQuery('databaseTest2','TestTable',columnsWithDataTypes,True)
```

Contents Of TestTableQuery variable:
```sql
IF OBJECT_ID('TestTable') IS Null
CREATE TABLE [TestDatabase].[dbo].[TestTable](
    name nvarchar(255) NOT NULL, 
    age int NULL , 
    location NVARCHAR(100) NULL
)
```
## executeQueries()
    executeQueries( connection(), listOfQueries, printQuery=False )

executeQueries() executes a list of query strings such as insert,create,update,etc. This function does not return a value it loops through a list of supplied queries and executes the query using the module pyodbc connection

```python
p2sql.executeQueries(connection,[TestTableQuery])
```

Any executable query can replace the variable [TestTableQuery]  
  
Example:  

```python
insertQuery = ["""
    INSERT INTO [databaseTest2].[dbo].[TestTable2]  
    (   
    name,   
    age,   
    location  
    )   
    VALUES   
    (   
        'Daivd',   
        '45',   
        'Detroit'  
    );  
"""]
```

```python
p2sql.executeQueries(connection,[insertQuery]) 
```
this will execute the custom created insert query 

Another way to insert values into a table without writing the query is to use the putInTable() function

## putInTable()
    putInTable( 
        connection(),
        tablename: str, 
        listOfColumnNames: list, 
        listOfListValues: list ,
        Database="" , 
        schema='dbo', 
        printQuery = False 
        )

putInTable() will create and run an insert execute query to the specified table, This function does not return a value, however the printQuery can be set to True to view the query that is inserted

```python
columnNamesList = ['name', 'age', 'location']
listOfListValues = [
    ['Daivd','45','Detroit'],
    ['Dora','45','Null']
    ]

p2sql.putInTable(connection,'TestTable2',columnNamesList, listOfListValues,'databaseTest2') 

```

## getFromTable()
    getFromTable( 
        connection(), t
        ablename: str,Database="", 
        printQuery = False, 
        listOfColumnNames = [ '' ] , 
        whereClause = [[]] , 
        schema='dbo' 
        )

getFromTable() will run a select query against the conneciotn and  return a list of tuples from the specified table 

Example:   
```python
[('Daivd', 45, 'Detroit'), ('Mike', 45, 'Null')]
```

```python
p2sql.getFromTable(connection,'TestTable2','databaseTest2')
```

# merge()
    merge(
        values: list, *example: [[]] 
        columns:list, 
        targetTable: str, 
        matchOnSource : list, 
        matchOnTarget : list, 
        WHEN_MATCHED_THEN_UPDATE_target = True, 
        NOT_MATCHED_BY_SOURCE_DELETE = True, 
        NOT_MATCHED_BY_SOURCE_query = "", 
        printQuery = False  
        )
        
merge() creates a MERGE query, which combines the INSERT, UPDATE, and the DELETE operations altogether, Creates a tempTable from the supplied lists 
```python

columns = ['name', 'age', 'location']

values = [
    ['Big Bird', '32', 'Seasame Street'], 
    ['Dora', '5', 'The Void']
    ]
```
<sub> Note: you can also use pandas DataFrame by using  tolist() on df values Example: 
```python
values = df.values.tolist()  
columns = df.columns.tolist()
```
```python
mergeQuery =  merge(values,columns,'TestTable',['name'],['name'])
```
The merge() function will return a MERGE query

```sql
 with DF_SQL_TBL_8675 as ( select * from(
    VALUES ('Big Bird','32','Seasame Street'),('Dora','5','The Void')) tempTable ([name],[age],[location])
    )

    MERGE TestTable AS Target
    USING  DF_SQL_TBL_8675      AS Source
    ON  Source.[name] = Target.[name]

    -- For Inserts
    WHEN NOT MATCHED BY Target THEN
        INSERT ([name],[age],[location])
        VALUES ( Source.[name] , Source.[age] , Source.[location] )

    -- For Updates
    WHEN MATCHED THEN UPDATE SET
     Target.[name] = Source.[name] , Target.[age] = Source.[age] , Target.[location] = Source.[location]
    -- For Deletes
    WHEN NOT MATCHED BY Source THEN DELETE;
```

# Pandas DataFrame Functions
The following functions will convert pandas DataFrames to SQL queries.

```python
import pandas as pd
```


## df_createTblQuery()

    df_createTblQuery( database: str, tablename: str, dataframe: pandas.DataFrame ,printQuery: bool, schema='dbo' )
  

df_createTblQuery(), takes the column headers from a pandas DataFrame and creates a SQL create Table query.
   
    Note: that this query will not insert data into the created table even if the DataFrame contains data for that you would need to use df_insertQuery(). All of the datatypes will be of type NVARCHAR(MAX)

```python
d = {'name': ["Big Bird", "Dora"], 'age': ['32', '5'], 'location': ['Seasame Street', 'The Void']  }

df = pd.DataFrame(data=d)

dataframeCreateQuery = p2sql.df_createTblQuery('databaseTest2','df_TestTable',df)

p2sql.executeQueries(connection,[dataframeCreateQuery],True)
```

## df_insertQuery()
    df_insertQuery( 
        database: str, 
        tablename: str, 
        dataframe: pandas.DataFrame, 
        printQuery= False, 
        schema='dbo' 
        )
  
df_insertQuery() will itterate through pandas dataframe and insert data into a sql database table. 
  
<sub>Note that the DataFrame column namnes need to match the database column names.

```python
d = {'name': ["Big Bird", "Dora"], 'age': ['32', '5'], 'location': ['Seasame Street', 'The Void']  }

df = pd.DataFrame(data=d)

dataframeQuery = p2sql.df_insertQuery('databaseTest2','df_TestTable',df)
```

results of variable dataframeQuery: 

```sql
INSERT INTO [databaseTest2].[dbo].[TestTable2](name , age , location  )VALUES('Big Bird','32','Seasame Street'); 
INSERT INTO [databaseTest2].[dbo].[TestTable2](name , age , location  )VALUES('Dora','5','The Void');
```
```python
p2sql.executeQueries(connection,[dataframeQuery],True)

p2sql.getFromTable(connection,"df_TestTable","databaseTest2",True)
```

## df_mergeQueryTables()
    df_mergeQuery(  
        database: str, 
        targetTable: str, 
        sourceTable: str, 
        target_source_columns: list,  
        dataframe: pandas.DataFrame, 
        mergeDelete: bool, 
        deleteStatement: str, 
        schema='dbo'  
        )  
  

df_mergeQueryTables() creates a merge query, merges data from source table to target table.  as long as the supplied dataframe matches the source and target table.
```sql
 WHEN target table ON target_source_columns  NOT MATCHED BY TARGET   
   then insert source table rows with insert query  
     
WHEN source table column MATCHED target  
    then update target table with update query  
      
WHEN NOT MATCHED BY SOURCE   
    THEN DELETE   
      
Unless mergeDelete is set to True,  
    then set deleteStatement to whatever query you want to run when not matched by source    
```
```python
df_mergeQueryTables = p2sql.df_mergeQuery("databaseTest2","df_TestTable","TestTable2",['name'],df,True,'')

p2sql.executeQueries(connection,[df_mergeQuery])
```




