-- Register the UDF JAR file
REGISTER '/usr/local/pig/lib/UpperCaseUDF.jar';

-- Define the custom UDF
DEFINE myUpperCase UpperCaseUDF();

-- Load your data (for example, emails)
emails = LOAD '/Users/sivaprasanth/Documents/Big Data/ex11/emails.txt' 
    USING PigStorage(',') 
    AS (UserID:chararray, From:chararray, To:bag{(recipient:chararray)});

-- Apply the UDF to convert 'From' field to uppercase
emails_upper = FOREACH emails GENERATE UserID, myUpperCase(From) AS From_Upper, FLATTEN(To);

-- Store the result in a new file
STORE emails_upper INTO '/Users/sivaprasanth/Documents/Big Data/ex11/emails_upper_output.txt' 
    USING PigStorage(',');
