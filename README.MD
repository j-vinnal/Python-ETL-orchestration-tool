## Python ETL orchestration tool using pyodbc ##
**This project was developed in 2022 to enhance my skills.**  

### Job Description ###

As Data Engineer you are requested to write a Python application in order to connect to Adventure Works database on SQL Server and do data manipulation.  

[Download AdventureWorks sample databases](https://docs.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver15&tabs=ssms)  

Steps include:  
• Restore AdventureWorks2019.bak database on SQL Server. It is important to use the 2019 OLTP version.  
• Connect to the database using pyodbc or any other library you prefer  
• Using tables Sales.SalesOrderHeader and Sales.SalesOrderDetail create and populate a Fact table called Fact.Sales to the best of your abilities  
• Using Sales.Customer and tables in Person schema create and populate a Dimension called Dim.Customer. You do not need to use all the tables in Person schema to create this dimension.  
• You can create Fact.Sales and Dim.Customer using either DDL or Python  

Notes:  
• Refer to AdventureWorks ERD.jpg which can help you in the above assignment  
• It is highly recommended to write structured and well documented code  

### I used Python to develop an ETL orchestration tool that can be used to create initial and incremental workflows.  


Schema creation is not part of the developed ETL process.
Run these queries manually before running the Python ETL script.

``` sql  
CREATE SCHEMA Fact  
GO  
CREATE SCHEMA Dim  
GO  

```  

### How to use / Example:

1. Restore AdventureWorks2019.bak database on SQL Server. It is important to use the 2019 OLTP version.  
2. Create manually schemas Fact and Dim.  
3. Open [start.py](start.py) and change DB parameters: server and db_name if needed.  

    Default DB parameters:
    ``` python 
    server = 'localhost,1433'
    db_name = 'AdventureWorks2019'
    ```  

4. Install the `pyodbc` library using pip:  
   ```bash
   pip install pyodbc
   ```   

5. Your first run is initial run, so execute those data load functions with parameter initial = True  

    ``` python 
    load_FactSales(True)
    load_DimCustomer(True)
    ```

    You should see the result:  
    ```
    Starting initial Fact.Sales load...
    Affected 121317 rows
    Starting initial Dim.Customer load...
    Affected 19119 rows
    ```  

6. To run an incremental load, execute the load functions with the parameter initial = False 

    ``` python
    load_FactSales(False)
    load_DimCustomer(False)
    ```  

    You should see result "Affected 0 rows" because there are no new or modified items:  
    ```
    Starting incremental Fact.Sales load...
    Affected 0 rows
    Starting incremental Dim.Customer load...
    Affected 0 rows
    ```  

    Run those queries to update some data in the source tables:  

    ``` sql  
    UPDATE Sales.SalesOrderDetail
    SET
          OrderQty =  789
        , ModifiedDate = CURRENT_TIMESTAMP
    WHERE SalesOrderDetailID = 1
    ;

    UPDATE Person.Person
    SET
        FirstName = 'Jüri'
        , MiddleName = ''
        , LastName = 'Vinnal'
        , ModifiedDate = CURRENT_TIMESTAMP
    WHERE BusinessEntityID = 17112
    ;
    ```

    Then run again ETL with initial = False parameters  

    You should see this result:  

    ```
    Starting incremental Fact.Sales load...
    Affected 1 rows
    Starting incremental Dim.Customer load...
    Affected 1 rows
    ```