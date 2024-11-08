-- Load the dataset from the file
emails = LOAD '/Users/sivaprasanth/Documents/Big Data/ex11/emails.txt' 
    USING PigStorage(',') 
    AS (UserID:chararray, From:chararray, To:bag{(recipient:chararray)});

-- Flatten the bag to get individual rows for each recipient
flattened_emails = FOREACH emails GENERATE UserID, From, FLATTEN(To);

-- Group by the sender (From) to list recipients for each sender
grouped_emails = GROUP flattened_emails BY From;

-- Create a list of recipients for each sender
recipient_list = FOREACH grouped_emails GENERATE group AS Sender, 
                                      FLATTEN(BagToString(flattened_emails.recipient)) AS Recipients;

-- Store the result in a new file
STORE recipient_list INTO '/Users/sivaprasanth/Documents/Big Data/ex11/sender_recipient_output.txt' 
    USING PigStorage(',');
