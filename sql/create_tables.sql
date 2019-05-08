--Create Actual Table
DROP TABLE {SCHEMA}.data_analyst_test;
CREATE TABLE {SCHEMA}.data_analyst_test
(packageName VARCHAR2(50),
 sku VARCHAR2(50),
 countryCode VARCHAR2(2),
 currency VARCHAR2(3),
 price NUMBER(10,2)
);
