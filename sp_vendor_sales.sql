CREATE OR REPLACE PROCEDURE sp_vendor_sales()
AS $$
BEGIN
INSERT INTO redshiftschema.vendor_sales(vendor,sales)
    SELECT productvendor,SUM(price*quantity) FROM redshiftspecschema.product AS products
    INNER JOIN redshiftspecschema.order AS orders
    ON products.productid=orders.productid
    GROUP BY productvendor;
END
$$ LANGUAGE plpgsql;