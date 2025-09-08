--Stored Procedures and Orchestra


USE SqlEtlSim;
GO

-- Extract
CREATE OR ALTER PROCEDURE dbo.spETL_Extract
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE staging.Customer_stg;
    TRUNCATE TABLE staging.Orders_stg;

    BULK INSERT staging.Customer_stg
    FROM 'C:\SqlEtlSim\data\incoming\customers.csv'
    WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);

    BULK INSERT staging.Orders_stg
    FROM 'C:\SqlEtlSim\data\incoming\orders.csv'
    WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
END
GO

-- Transform & Load
CREATE OR ALTER PROCEDURE dbo.spETL_TransformLoad
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Upserts INT = 0;

    WITH CustRaw AS (
        SELECT TRY_CAST(CustomerID AS INT) AS CustomerID,
               LTRIM(RTRIM(FirstName)) AS FirstName,
               LTRIM(RTRIM(LastName)) AS LastName,
               NULLIF(LTRIM(RTRIM(Email)), '') AS Email,
               NULLIF(LTRIM(RTRIM(Country)), '') AS Country
        FROM staging.Customer_stg
    ), CustRank AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY (SELECT 0)) AS rn
        FROM CustRaw WHERE CustomerID IS NOT NULL
    )
    SELECT * INTO #Customers_Clean FROM CustRank WHERE rn = 1;

    WITH OrdersRaw AS (
        SELECT TRY_CAST(OrderID AS INT) AS OrderID,
               TRY_CAST(CustomerID AS INT) AS CustomerID,
               TRY_CONVERT(DATE, REPLACE(OrderDate,'/','-'), 23) AS OrderDate,
               TRY_CAST(Amount AS DECIMAL(18,2)) AS Amount
        FROM staging.Orders_stg
    ), OrdersValid AS (
        SELECT * FROM OrdersRaw
        WHERE OrderID IS NOT NULL AND CustomerID IS NOT NULL
          AND OrderDate IS NOT NULL AND Amount IS NOT NULL
    )
    SELECT * INTO #Orders_Clean FROM OrdersValid;

    IF OBJECT_ID('tempdb..#merge_out') IS NOT NULL DROP TABLE #merge_out;
    CREATE TABLE #merge_out(action NVARCHAR(10));

    MERGE dw.DimCustomer AS tgt
    USING #Customers_Clean AS src
    ON tgt.CustomerID = src.CustomerID
    WHEN MATCHED THEN UPDATE SET
        tgt.FirstName = src.FirstName,
        tgt.LastName  = src.LastName,
        tgt.Email     = src.Email,
        tgt.Country   = src.Country
    WHEN NOT MATCHED THEN INSERT (CustomerID, FirstName, LastName, Email, Country)
    VALUES (src.CustomerID, src.FirstName, src.LastName, src.Email, src.Country)
    OUTPUT $action INTO #merge_out(action);

    SELECT @Upserts = COUNT(*) FROM #merge_out;

    ;WITH O AS (
        SELECT o.OrderID, d.CustomerKey, o.OrderDate, o.Amount
        FROM #Orders_Clean o
        INNER JOIN dw.DimCustomer d ON d.CustomerID = o.CustomerID
    )
    INSERT INTO dw.FactOrder (OrderID, CustomerKey, OrderDate, Amount)
    SELECT o.OrderID, o.CustomerKey, o.OrderDate, o.Amount
    FROM O o
    LEFT JOIN dw.FactOrder f ON f.OrderID = o.OrderID
    WHERE f.OrderID IS NULL;

    SELECT 
        (SELECT COUNT(*) FROM staging.Customer_stg) AS RowsStagingCustomers,
        (SELECT COUNT(*) FROM staging.Orders_stg) AS RowsStagingOrders,
        @Upserts AS RowsDimCustomerUpserts,
        @@ROWCOUNT AS RowsFactInserts;
END
GO

-- Orchestrator with logging
CREATE OR ALTER PROCEDURE dbo.spETL_Run
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RunID INT, @err NVARCHAR(MAX);

    INSERT INTO audit.ETL_Run DEFAULT VALUES;
    SET @RunID = SCOPE_IDENTITY();

    BEGIN TRY
        EXEC dbo.spETL_Extract;
        DECLARE @Rows TABLE (
            RowsStagingCustomers INT,
            RowsStagingOrders INT,
            RowsDimCustomerUpserts INT,
            RowsFactInserts INT
        );
        INSERT INTO @Rows EXEC dbo.spETL_TransformLoad;

        UPDATE audit.ETL_Run
        SET EndTime = SYSUTCDATETIME(),
            Status = 'Success',
            RowsStagingCustomers = (SELECT TOP 1 RowsStagingCustomers FROM @Rows),
            RowsStagingOrders = (SELECT TOP 1 RowsStagingOrders FROM @Rows),
            RowsDimCustomerUpserts = (SELECT TOP 1 RowsDimCustomerUpserts FROM @Rows),
            RowsFactInserts = (SELECT TOP 1 RowsFactInserts FROM @Rows)
        WHERE RunID = @RunID;
    END TRY
    BEGIN CATCH
        SET @err = CONCAT('Msg ', ERROR_NUMBER(), ' Line ', ERROR_LINE(), ': ', ERROR_MESSAGE());
        INSERT INTO audit.ETL_Error(RunID, StepName, Detail) VALUES(@RunID, 'spETL_Run', @err);
        UPDATE audit.ETL_Run SET EndTime = SYSUTCDATETIME(), Status = 'Failed', ErrorMessage = @err WHERE RunID = @RunID;
        THROW;
    END CATCH
END
GO
