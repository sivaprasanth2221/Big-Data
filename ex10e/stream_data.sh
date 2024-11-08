#!/bin/bash

# Kafka topic and broker details
TOPIC="data-stream"
BROKER="localhost:9092"

# Read dataset.csv line by line and send each line to Kafka
while IFS= read -r line
do
    echo "$line" | kafka-console-producer --topic data-stream --bootstrap-server localhost:9092
    sleep 1  # Optional: Add a delay between messages (adjust as needed)
done < dataset.csv
