#!/usr/bin/env bash
# EXPERIMENT 15 — KAFKA TOPIC, PRODUCER AND CONSUMER
# Aim: Create a topic and verify messages sent by a producer arrive at a consumer.
# Run commands from the Kafka installation directory; separate terminals are required.

# IMPORTANT: This modern KRaft initialization is for a fresh modern Kafka installation only.
# Do not format an existing Kafka data directory. Older CDH Kafka may require ZooKeeper commands.
# One-time setup for a compatible modern Kafka release:
# KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
# bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties

# TERMINAL 1 — start the broker and leave it running.
# bin/kafka-server-start.sh config/server.properties

# TERMINAL 2 — create/list the topic once the broker is available.
# bin/kafka-topics.sh --create --topic wordcount-topic \
#   --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
# bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# TERMINAL 3 — start the producer and type messages, pressing Enter after each.
# bin/kafka-console-producer.sh --topic wordcount-topic \
#   --bootstrap-server localhost:9092

# TERMINAL 4 — start the consumer and observe messages.
# bin/kafka-console-consumer.sh --topic wordcount-topic \
#   --from-beginning --bootstrap-server localhost:9092

# Expected: consumer prints each produced message.
# Stop processes with Ctrl+C. Match all commands to the installed Kafka version.
