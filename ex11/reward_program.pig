-- Load the customer dataset
customers = LOAD '/Users/sivaprasanth/Documents/Big Data/ex11/customer_ltv.txt' 
    USING PigStorage(',') 
    AS (Customer_Name:chararray, LifeTimeValue:int);

-- Split customers into Silver and Gold programs based on their lifetime values
silver_program = FILTER customers BY LifeTimeValue > 100 AND LifeTimeValue <= 20000;
gold_program = FILTER customers BY LifeTimeValue > 20000;

-- Display the customers in Silver and Gold Programs
DUMP silver_program;
DUMP gold_program;

