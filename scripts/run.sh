#!/usr/bin/env bash

IMAGE_REPOSITORY=${IMAGE_REPOSITORY:-quay.io/redhatdemo/2021-kafka-streams-player-matches-aggregator:latest}

docker run \
--rm \
--name kafka-streams-player-matches-aggregator \
-e LOG_LEVEL=TRACE \
-e KAFKA_SVC_USERNAME=$KAFKA_SVC_USERNAME \
-e KAFKA_SVC_PASSWORD=$KAFKA_SVC_PASSWORD \
-e KAFKA_BOOTSTRAP_URL=$KAFKA_BOOTSTRAP_URL \
-p 8080:8080 \
$IMAGE_REPOSITORY
