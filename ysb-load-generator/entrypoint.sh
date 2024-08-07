#!/usr/bin/env bash

# Create new campaign
lein run -n

# Echo load generator ENV variables
echo "Starting load generator with the following ENV variables:"
echo "KAFKA_BROKER: $KAFKA_BROKER"
echo "KAFKA_PORT: $KAFKA_PORT"
echo "KAFKA_TOPIC: $KAFKA_TOPIC"
echo "ZOOKEEPER_HOST: $ZOOKEEPER_HOST"
echo "ZOOKEEPER_PORT: $ZOOKEEPER_PORT"
echo "REDIS_HOST: $REDIS_HOST"
echo "LOAD: $LOAD"
echo "SKEW_ENABLED: $SKEW_ENABLED"

# IF SKEW_ENABLED is set to true, start skew generator else start normal
if [ "$SKEW_ENABLED" = "true" ]; then
    echo "lein run -r -t $LOAD -w"
    lein run -r -t $LOAD -w
else
    echo "lein run -t $LOAD"
    lein run -r -t $LOAD
fi
