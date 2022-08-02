-- Creating external schema for redshift spectrum
CREATE EXTERNAL SCHEMA redshiftspecschema
FROM DATA CATALOG
DATABASE 'dev'
IAM_ROLE 'arn:aws:iam::211850853456:role/redshift-etl-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Creating external redshift spectrum tables 

CREATE EXTERNAL TABLE redshiftspecschema.customer(
    customerid int,
    customername nvarchar(100),
    customeroccupation nvarchar(100)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://datalake-idp/raw/customers/'


CREATE EXTERNAL TABLE redshiftspecschema.product(
    productid int,
    productregion nvarchar(100),
    productvendor nvarchar(100)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://datalake-idp/raw/products/'


CREATE EXTERNAL TABLE redshiftspecschema.order(
    orderid int,
    customerid int,
    productid int,
    price int,
    quantity int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://datalake-idp/raw/orders/'

-- Create internal schema for internal redshift tables
CREATE SCHEMA IF NOT EXISTS redshiftschema;

-- Creating internal redshift specific tables

CREATE TABLE redshiftschema.occupation_sales(
    occupation varchar(100) not null,
    sales integer not null
)

CREATE TABLE redshiftschema.vendor_sales(
    vendor varchar(100) not null,
    sales integer not null
)

CREATE TABLE redshiftschema.geographical_sales(
    region varchar(100) not null,
    sales integer not null
)