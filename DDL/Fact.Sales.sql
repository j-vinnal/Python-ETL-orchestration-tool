IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = 'Sales' AND s.name = 'Fact')
TRUNCATE TABLE Fact.Sales;

IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = 'Sales' AND s.name = 'Fact')
CREATE TABLE Fact.Sales
(
    SalesOrderDetailKey             INTEGER PRIMARY KEY,
    SalesOrderKey                   INTEGER,
    ProductKey                      INTEGER,
    CustomerKey                     INTEGER,
    Status                          VARCHAR(255),
    SalesOrderDateKey               INTEGER,
    SalesOrderDueDateKey            INTEGER,
    SalesOrderShipDateKey           INTEGER,
    OrderQty                        INTEGER,
    UnitPrice                       MONEY,
    UnitPriceDiscount               MONEY,
    LineTotal                       NUMERIC(38, 6),
    SalesOrderSubTotal              MONEY,
    SalesOrderTaxAmount             MONEY,
    SalesOrderFreightAmount         MONEY,
    SalesOrderTotalDueAmount        MONEY,
    ModifiedDate                    DATETIME
);