package org.acme.kafka.streams.aggregator.streams;


import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.acme.kafka.streams.aggregator.model.Aggregate;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.jboss.logging.Logger;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


@ApplicationScoped
public class TopologyProducer {
    private static final Logger LOG = Logger.getLogger(TopologyProducer.class);

    // All match events are written to this topic. These events have varying structure
    // so we do a little magic and filtering below to manage that complexity
    static final String MATCHES_TOPIC = "match-updates";
    static final String PLAYER_MATCHES_INTERMEDIARY_TOPIC = "player-matches-intermediary";
    static final String PLAYER_MATCHES_AGGREGATE_TOPIC = "player-matches-aggregated";
    static final String PLAYER_MATCHES_STORE = "player-matches-store";

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KeyValueBytesStoreSupplier playerMatchesAggregateStoreSupplier = Stores.persistentKeyValueStore(PLAYER_MATCHES_STORE);

        builder.stream(
            MATCHES_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
            )
            .filter((k, v) -> {
                JsonObject json = new JsonObject(v);
                // Ignore anything that's not an "attack". There's no
                // point in recording a player if they never attacked
                return json.getString("type").equals(Aggregate.PAYLOAD_ATTACK);
            })
            .map((k, v) -> {
                JsonObject json = new JsonObject(v);
                String gameId = Aggregate.getGameIdFromJson(json);
                String playerId = Aggregate.getPlayerIdFromAttackJson(json);
                String newId = gameId + ":" + playerId;

                LOG.debug("Keyed entry to: " + newId);

                return KeyValue.pair(newId, v);
            })
            .through(
                PLAYER_MATCHES_INTERMEDIARY_TOPIC,
                Produced.with(Serdes.String(), Serdes.String())
            )
            .groupByKey()
            .aggregate(
                String::new,
                (key, value, aggregate) -> {
                    JsonObject aggregateJson;
                    JsonObject incoming = new JsonObject(value);

                    if (aggregate.length() > 0) {
                        // Create a JSON object from existing aggregate data
                        aggregateJson = new JsonObject(aggregate);
                    } else {
                        // Create a new empty JSON object
                        aggregateJson = new JsonObject();
                        aggregateJson.put("gameId", Aggregate.getGameIdFromJson(incoming));
                        aggregateJson.put("playerId", Aggregate.getPlayerIdFromAttackJson(incoming));
                        aggregateJson.put("matches", new JsonArray());
                    }

                    String matchId = Aggregate.getMatchIdFromJson(incoming);
                    JsonArray matches = aggregateJson.getJsonArray("matches");

                    if (matches.contains(matchId) == false) {
                        matches.add(matchId);
                    }

                    String newJson = aggregateJson.encode();

                    LOG.debug("Updated aggregate\"" + key + "\" - " + newJson);
                    return newJson;
                },
                Materialized.<String, String>as(playerMatchesAggregateStoreSupplier)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
            )
            .toStream()
            .to(
                PLAYER_MATCHES_AGGREGATE_TOPIC,
                Produced.with(Serdes.String(), Serdes.String())
            );

            return builder.build();
    }
}
