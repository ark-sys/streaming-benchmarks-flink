# Create leiningen environment for streaming-benchmark load generator
FROM clojure:lein-2.11.2

# Default environment variables
ENV KAFKA_BROKER=kafka
ENV KAFKA_PORT=9092
ENV KAFKA_TOPIC=ad-events
ENV ZOOKEEPER_HOST=zookeeper
ENV ZOOKEEPER_PORT=2181
ENV REDIS_HOST=redis
ENV LOAD=1000
ENV SKEW_ENABLED=false

# Add clojure script to container
ADD data /data

# Set working directory
WORKDIR /data

# Copy entrypoint script
ADD entrypoint.sh /entrypoint.sh

# Make entrypoint script executable
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

