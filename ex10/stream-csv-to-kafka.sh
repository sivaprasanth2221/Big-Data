# stream-csv-to-kafka.sh
# Adjust the file path to your CSV file
while IFS= read -r line; do
    echo "$line" | bin/kafka-console-producer.sh --topic test-stream --bootstrap-server localhost:9092
done < streaming_data.csv
