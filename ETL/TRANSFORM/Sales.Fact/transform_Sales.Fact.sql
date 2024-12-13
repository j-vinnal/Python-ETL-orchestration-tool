IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = 'TempSales' AND s.name = 'Fact')
DROP TABLE Fact.TempSales;

SELECT
      sd.SalesOrderDetailID AS SalesOrderDetailKey
    , sh.SalesOrderID AS SalesOrderKey
    , sd.ProductID AS ProductKey
    , sh.CustomerID AS CustomerKey
    , CASE
        WHEN sh.Status = 1
            THEN 'In process'
        WHEN sh.Status = 2
            THEN 'Approved'
        WHEN sh.Status = 3
            THEN 'Backordered'
        WHEN sh.Status = 4
            THEN 'Rejected'
        WHEN sh.Status = 5
            THEN 'Shipped'
        WHEN sh.Status = 6
            THEN 'Cancelled'
    END AS Status
    , CONVERT(INT, CONVERT(VARCHAR(8),sh.OrderDate,112)) AS SalesOrderDateKey
    , CONVERT(INT, CONVERT(VARCHAR(8),sh.DueDate,112)) AS SalesOrderDueDateKey
    , CONVERT(INT, CONVERT(VARCHAR(8),sh.ShipDate,112)) AS SalesOrderShipDateKey
    , sd.OrderQty
    , sd.UnitPrice
    , sd.UnitPriceDiscount
    , sd.LineTotal
    , sh.SubTotal AS SalesOrderSubTotal
    , sh.TaxAmt AS SalesOrderTaxAmount
    , sh.Freight AS SalesOrderFreightAmount
    , sh.TotalDue AS SalesOrderTotalDueAmount
    , sd.ModifiedDate
INTO Fact.TempSales
FROM Sales.SalesOrderDetail sd
LEFT JOIN Sales.SalesOrderHeader sh  ON sd.SalesOrderID = sh.SalesOrderID
WHERE sd.ModifiedDate >= (SELECT COALESCE(LastLoadedTS, CONVERT(DATETIME, '1900-01-01', 102)) FROM Technical.IncrementalLoadWindow
                                                WHERE TableSchema = 'Fact'
                                                    AND TableName = 'Sales')
;

DELETE FROM Fact.Sales WHERE EXISTS (SELECT 1 FROM Fact.TempSales WHERE TempSales.SalesOrderDetailKey = Sales.SalesOrderDetailKey);

INSERT INTO Fact.Sales
(
     SalesOrderDetailKey
    , SalesOrderKey
    , ProductKey
    , CustomerKey
    , Status
    , SalesOrderDateKey
    , SalesOrderDueDateKey
    , SalesOrderShipDateKey
    , OrderQty
    , UnitPrice
    , UnitPriceDiscount
    , LineTotal
    , SalesOrderSubTotal
    , SalesOrderTaxAmount
    , SalesOrderFreightAmount
    , SalesOrderTotalDueAmount
    , ModifiedDate
)
SELECT
      SalesOrderDetailKey
    , SalesOrderKey
    , ProductKey
    , CustomerKey
    , Status
    , SalesOrderDateKey
    , SalesOrderDueDateKey
    , SalesOrderShipDateKey
    , OrderQty
    , UnitPrice
    , UnitPriceDiscount
    , LineTotal
    , SalesOrderSubTotal
    , SalesOrderTaxAmount
    , SalesOrderFreightAmount
    , SalesOrderTotalDueAmount
    , ModifiedDate
FROM Fact.TempSales;