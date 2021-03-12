# 2021 Kafka Streams Player Matches Aggregator

This application will receive match events via a Kafka Topic, and aggregate
them into a per player record that contains the matches that the player has
played.

The example below is for a player who has played two matches:

```json
{
  "gameId": "qohs1SNJwul3R0746IYOn",
  "playerId":"7S724N3jjIDdUQH3XxDMX",
  "matches":["2YFdCqkRoTBQsTCLDNTyl", "FZNQcjGn_kg8c7JV6qHzJ"]
}
```

## HTTP API for Player Data

To fetch data for a given player you must pass a `gameId` and `playerId` as URL
parameters. A sample curl and response is shown below:

```
export GAME_ID=qohs1SNJwul3R0746IYOn
export PLAYER_ID=7S724N3jjIDdUQH3XxDMX
curl http://localhost:8080/game/$GAME_ID/player-matches/$PLAYER_ID

{"gameId":"qohs1SNJwul3R0746IYOn","playerId":"7S724N3jjIDdUQH3XxDMX","matches":["2YFdCqkRoTBQsTCLDNTyl"]}
```

## Building

To build a container image using Docker:

```bash
./scripts/build.sh
```

## Running

```
./scripts/build.sh

export KAFKA_SVC_USERNAME=username
export KAFKA_SVC_PASSWORD=username
export KAFKA_BOOTSTRAP_URL=hostname.kafka.devshift.org:443

./scripts/run.sh
```

## Running Locally

For development purposes it can be handy to run _aggregator_ application
directly on your local machine instead of via Docker.
For that purpose, a separate Docker Compose file is provided which just starts Apache Kafka and ZooKeeper, _docker-compose-local.yaml_
configured to be accessible from your host system.
Open this file an editor and change the value of the `KAFKA_ADVERTISED_LISTENERS` variable so it contains your host machine's name or ip address.
Then run:

```bash
docker-compose -f docker-compose-local.yaml up

mvn quarkus:dev -Dquarkus.http.port=8081 -f aggregator/pom.xml
```

Any changes done to the _aggregator_ application will be picked up instantly,
and a reload of the stream processing application will be triggered upon the next Kafka message to be processed.
