--Loading dta from CSVs



USE SqlEtlSim;
GO

TRUNCATE TABLE staging.Customer_stg;
TRUNCATE TABLE staging.Orders_stg;

-- Load Customers
BULK INSERT staging.Customer_stg
FROM 'C:\SqlEtlSim\data\incoming\customers.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- Load Orders
BULK INSERT staging.Orders_stg
FROM 'C:\SqlEtlSim\data\incoming\orders.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
