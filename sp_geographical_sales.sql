CREATE OR REPLACE PROCEDURE sp_geographical_sales()
AS $$
BEGIN
INSERT INTO redshiftschema.geographical_sales(region,sales)
    SELECT productregion,SUM(price*quantity) FROM redshiftspecschema.product AS products
    INNER JOIN redshiftspecschema.order AS orders
    ON products.productid=orders.orderid
    GROUP BY productregion;
END
$$ LANGUAGE plpgsql