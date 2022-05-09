#!/bin/bash
export KAFKA_HOST="localhost:9092"
export KAFKA_TOPIC="TOPICO_1"
export KAFKA_GROUP_ID_READER="consumer_1"

export className=App
echo "## Running $className..."
mvn exec:java -Dexec.mainClass="br.com.julioneto.kafkaconsumer.$className"