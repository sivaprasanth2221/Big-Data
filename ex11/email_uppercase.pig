-- Register the Piggy Bank JAR file
REGISTER '/usr/local/pig/contrib/piggybank/java/piggybank.jar';

-- Import the Piggy Bank eval function for string manipulation
DEFINE ToUpper org.apache.pig.piggybank.evaluation.string.UPPER();

-- Load the dataset
emails = LOAD '/Users/sivaprasanth/Documents/Big Data/ex11/emails.txt' 
    USING PigStorage(',') 
    AS (UserID:chararray, From:chararray, To:bag{(recipient:chararray)});

-- Flatten the 'To' field and apply UPPER function
emails_flatten = FOREACH emails GENERATE UserID, ToUpper(From) AS From_Upper, FLATTEN(To) AS recipient;
emails_upper = FOREACH emails_flatten GENERATE UserID, From_Upper, ToUpper(recipient) AS To_Upper;

-- Store the result in a new file
STORE emails_upper INTO '/Users/sivaprasanth/Documents/Big Data/ex11/emails_upper_output.txt' 
    USING PigStorage(',');
