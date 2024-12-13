IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = 'TempCustomer_st' AND s.name = 'Dim')
DROP TABLE Dim.TempCustomer_st;

SELECT
      cu.CustomerID AS CustomerKey
    , p.PersonType AS CustomerType
    , p.Title
    , p.FirstName
    , p.MiddleName
    , p.LastName
    , p.EmailPromotion
    , pe.EmailAddress
    , p.ModifiedDate
INTO Dim.TempCustomer_st
FROM Sales.Customer cu
LEFT JOIN Person.Person p ON p.BusinessEntityID = cu.PersonID
LEFT JOIN Person.EmailAddress pe ON p.BusinessEntityID = pe.BusinessEntityID
WHERE p.ModifiedDate >= (SELECT COALESCE(LastLoadedTS, CONVERT(DATETIME, '1900-01-01', 102)) FROM Technical.IncrementalLoadWindow
                                                WHERE TableSchema = 'Dim'
                                                    AND TableName = 'Customer')
;


IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = 'TempCustomer' AND s.name = 'Dim')
DROP TABLE Dim.TempCustomer;


SELECT
      st.CustomerKey
    , st.CustomerType
    , st.Title
    , st.FirstName
    , st.MiddleName
    , st.LastName
    , st.EmailPromotion
    , st.EmailAddress
    , st.ModifiedDate
    , CASE
    	WHEN cu.CustomerKey IS NULL
		    THEN 2 --INSERT
        WHEN cu.CustomerKey IS NOT NULL
			AND NOT cu.CustomerType <> st.CustomerType
			AND NOT cu.Title <> st.Title
			AND NOT cu.FirstName <> st.FirstName
			AND NOT cu.MiddleName <> st.MiddleName
			AND NOT cu.LastName <> st.LastName
			AND NOT cu.EmailPromotion <> st.EmailPromotion
			AND NOT cu.EmailAddress <> st.EmailAddress
			AND NOT cu.ModifiedDate <> st.ModifiedDate
			THEN 0 --DO_NOTHING
        ELSE 1 --UPDATE
	END AS has_changes_flag
INTO Dim.TempCustomer
FROM Dim.TempCustomer_st st
LEFT JOIN Dim.Customer cu ON cu.CustomerKey = st.CustomerKey
;

-------UPDATE
UPDATE Dim.Customer SET
	      Customer.CustomerType = tmp.CustomerType
	    , Customer.Title = tmp.Title
	    , Customer.FirstName = tmp.FirstName
	    , Customer.MiddleName = tmp.MiddleName
	    , Customer.LastName = tmp.LastName
	    , Customer.EmailPromotion = tmp.EmailPromotion
	    , Customer.EmailAddress = tmp.EmailAddress
	    , Customer.ModifiedDate = tmp.ModifiedDate
FROM Dim.TempCustomer tmp
WHERE 1=1
    AND tmp.CustomerKey = Customer.CustomerKey
    AND tmp.has_changes_flag = 1;

---INSERT
INSERT INTO Dim.Customer
    (
        CustomerKey
      , CustomerType
      , Title
      , FirstName
      , MiddleName
      , LastName
      , EmailPromotion
      , EmailAddress
      , ModifiedDate
    )
SELECT
       CustomerKey
     , CustomerType
     , Title
     , FirstName
     , MiddleName
     , LastName
     , EmailPromotion
     , EmailAddress
     , ModifiedDate
FROM Dim.TempCustomer
WHERE 1=1
  AND has_changes_flag = 2
;


