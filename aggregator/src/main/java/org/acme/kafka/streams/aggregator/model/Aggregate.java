package org.acme.kafka.streams.aggregator.model;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.json.JsonObject;

@RegisterForReflection
public class Aggregate {
    final public static String INCOMING_KEY_GAME_ID = "game";
    final public static String INCOMING_KEY_MATCH_ID = "match";
    final public static String INCOMING_KEY_PLAYER_UUID = "uuid";
    final public static String INCOMING_KEY_ATTACK_BY = "by";

    final public static String PAYLOAD_ATTACK = "attack";

    public final static String getGameIdFromJson (JsonObject json) {
        return json.getJsonObject("data").getString(Aggregate.INCOMING_KEY_GAME_ID);
    }

    public final static String getPlayerIdFromAttackJson (JsonObject json) {
        return json
            .getJsonObject("data")
            .getJsonObject(Aggregate.INCOMING_KEY_ATTACK_BY)
            .getString(Aggregate.INCOMING_KEY_PLAYER_UUID);
    }

    public final static String getMatchIdFromJson (JsonObject json) {
        return json
            .getJsonObject("data")
            .getString(Aggregate.INCOMING_KEY_MATCH_ID);
    }
}
