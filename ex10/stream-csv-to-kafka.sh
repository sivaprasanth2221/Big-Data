# stream-csv-to-kafka.sh
# Adjust the file path to your CSV file
while IFS= read -r line; do
    echo "$line" | kafka-console-producer --topic test-stream --bootstrap-server localhost:9092
done < /Users/sivaprasanth/Documents/Big\ Data/ex10/streaming_data.csv
