IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = 'Customer' AND s.name = 'Dim')
TRUNCATE TABLE Dim.Customer;

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = 'Customer' AND s.name = 'Dim')
CREATE TABLE Dim.Customer
(
    CustomerKey    INTEGER PRIMARY KEY,
    CustomerType   CHAR(2),
    Title          VARCHAR(8),
    FirstName      NAME,
    MiddleName     NAME,
    LastName       NAME,
    EmailPromotion INTEGER,
    EmailAddress   VARCHAR(50),
    ModifiedDate   DATETIME
)
;