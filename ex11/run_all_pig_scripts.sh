#!/bin/bash

pig -x mapreduce /Users/sivaprasanth/Documents/Big Data/ex11/extract_domain.pig
pig -x mapreduce /Users/sivaprasanth/Documents/Big Data/ex11/email_processing.pig
pig -x mapreduce /Users/sivaprasanth/Documents/Big Data/ex11/email_uppercase.pig
pig -x mapreduce /Users/sivaprasanth/Documents/Big Data/ex11/reward_program.pig
pig -x mapreduce /Users/sivaprasanth/Documents/Big Data/ex11/order_customer_analysis.pig
