-- Register the Python UDF
REGISTER 'EmailDomain.py' USING jython AS myudf;

-- Load the dataset
emails = LOAD '/Users/sivaprasanth/Documents/Big Data/ex11/emails.txt' 
    USING PigStorage(',') 
    AS (UserID:chararray, From:chararray, To:bag{(recipient:chararray)});

-- Apply the UDF to extract the domain of the email addresses
emails_with_domain = FOREACH emails GENERATE UserID, myudf.extract_domain(From) AS From_Domain, 
                     FLATTEN(To);

-- Store the result in a new file
STORE emails_with_domain INTO '/Users/sivaprasanth/Documents/Big Data/ex11/emails_with_domain_output.txt' 
    USING PigStorage(',');
