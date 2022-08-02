CREATE OR REPLACE PROCEDURE sp_occupation_sales()
AS $$
BEGIN
INSERT INTO redshiftschema.occupation_sales(occupation,sales) 
    SELECT customeroccupation,SUM(price*quantity) FROM redshiftspecschema.customer AS customers
    INNER JOIN redshiftspecschema.order AS orders
    ON customers.customerid = orders.customerid 
    GROUP BY customeroccupation;
END
$$ LANGUAGE plpgsql;