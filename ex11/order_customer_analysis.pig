-- Load the Order data
orders = LOAD '/Users/sivaprasanth/Documents/Big Data/ex11/orders.txt' 
    USING PigStorage(',') 
    AS (Order_ID:int, Customer_ID:int, ItemID:int, Item_Name:chararray, 
        Order_Date:chararray, Delivery_Date:chararray, Quantity:int, Cost:int);

-- Load the Customer data
customers = LOAD '/Users/sivaprasanth/Documents/Big Data/ex11/customers.txt' 
    USING PigStorage(',') 
    AS (Customer_ID:int, Customer_Name:chararray, Address:chararray, 
        City:chararray, State:chararray, Country:chararray);

-- Group by Customer_ID to determine number of items bought
grouped_orders = GROUP orders BY Customer_ID;
items_bought = FOREACH grouped_orders GENERATE group AS Customer_ID, SUM(orders.Quantity) AS Total_Items;

-- Join orders and customers on Customer_ID
merged_data = JOIN orders BY Customer_ID, customers BY Customer_ID;

-- Store the merged data in a new file
STORE merged_data INTO '/Users/sivaprasanth/Documents/Big Data/ex11/merged_output.txt' 
    USING PigStorage(',');

-- Create a map of <Order_ID, Customer_Name>
order_customer_map = FOREACH merged_data GENERATE orders::Order_ID, customers::Customer_Name;

-- Dump the output to see the results
DUMP order_customer_map;

