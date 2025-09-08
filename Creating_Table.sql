--Creating Table

USE SqlEtlSim;
GO

-- Staging tables
CREATE TABLE staging.Customer_stg (
    CustomerID INT NULL,
    FirstName NVARCHAR(50) NULL,
    LastName NVARCHAR(50) NULL,
    Email NVARCHAR(100) NULL,
    Country NVARCHAR(50) NULL
);

CREATE TABLE staging.Orders_stg (
    OrderID INT NULL,
    CustomerID INT NULL,
    OrderDate NVARCHAR(25) NULL,
    Amount NVARCHAR(25) NULL
);

-- Dimension table
CREATE TABLE dw.DimCustomer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT NOT NULL UNIQUE,
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    Email NVARCHAR(100) NULL,
    Country NVARCHAR(50) NULL,
    LoadDts DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);

-- Fact table
CREATE TABLE dw.FactOrder (
    OrderKey INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT NOT NULL UNIQUE,
    CustomerKey INT NOT NULL,
    OrderDate DATE NOT NULL,
    Amount DECIMAL(18,2) NOT NULL,
    LoadDts DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_FactOrder_DimCustomer FOREIGN KEY (CustomerKey)
        REFERENCES dw.DimCustomer(CustomerKey)
);

-- Audit tables
CREATE TABLE audit.ETL_Run (
    RunID INT IDENTITY(1,1) PRIMARY KEY,
    StartTime DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
    EndTime DATETIME2(0) NULL,
    Status NVARCHAR(20) NOT NULL DEFAULT 'Started',
    RowsStagingCustomers INT NULL,
    RowsStagingOrders INT NULL,
    RowsDimCustomerUpserts INT NULL,
    RowsFactInserts INT NULL,
    ErrorMessage NVARCHAR(MAX) NULL
);

CREATE TABLE audit.ETL_Error (
    ErrorID INT IDENTITY(1,1) PRIMARY KEY,
    RunID INT NULL,
    StepName NVARCHAR(100) NOT NULL,
    Detail NVARCHAR(MAX) NOT NULL,
    ErrorTime DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);
