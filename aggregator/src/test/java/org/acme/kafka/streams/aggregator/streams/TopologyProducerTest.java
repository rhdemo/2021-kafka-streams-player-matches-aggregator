package org.acme.kafka.streams.aggregator.streams;

import static org.acme.kafka.streams.aggregator.streams.TopologyProducer.MATCHES_TOPIC;
import static org.acme.kafka.streams.aggregator.streams.TopologyProducer.PLAYER_MATCHES_STORE;
import static org.acme.kafka.streams.aggregator.streams.TopologyProducer.PLAYER_MATCHES_AGGREGATE_TOPIC;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import javax.inject.Inject;

import org.acme.kafka.streams.aggregator.model.Aggregate;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Testing of the Topology without a broker, using TopologyTestDriver
 */
@QuarkusTest
public class TopologyProducerTest {

    @Inject
    Topology topology;

    TopologyTestDriver testDriver;

    TestInputTopic<String, String> matches;

    TestOutputTopic<String, String> aggregates;

    @BeforeEach
    public void setUp() throws Exception {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "playerMatchesAggregate");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(topology, config);

        matches = testDriver.createInputTopic(MATCHES_TOPIC, new StringSerializer(), new StringSerializer());
        aggregates = testDriver.createOutputTopic(PLAYER_MATCHES_AGGREGATE_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    public void tearDown(){
        testDriver.getTimestampedKeyValueStore(PLAYER_MATCHES_STORE).flush();
        testDriver.close();
    }


    @Test
    public void testPlayerMatchesAggregateTopology () throws Exception {
        JsonObject matchStartJson = new JsonObject(readFileAsString("src/test/resources/match-start.json"));
        JsonObject matchJsonA = new JsonObject(readFileAsString("src/test/resources/attack.json"));
        JsonObject matchJsonB1 = new JsonObject(readFileAsString("src/test/resources/attack.json"));
        JsonObject matchJsonB2 = new JsonObject(readFileAsString("src/test/resources/attack.json"));
        JsonObject matchJsonC = new JsonObject(readFileAsString("src/test/resources/attack.json"));

        matchJsonA.getJsonObject("data").put(Aggregate.INCOMING_KEY_MATCH_ID, "A");
        matchJsonB1.getJsonObject("data").put(Aggregate.INCOMING_KEY_MATCH_ID, "B");
        matchJsonB2.getJsonObject("data").put(Aggregate.INCOMING_KEY_MATCH_ID, "B");
        matchJsonB2.getJsonObject("data").put(Aggregate.INCOMING_KEY_MATCH_ID, "B");
        matchJsonC.getJsonObject("data").put(Aggregate.INCOMING_KEY_MATCH_ID, "C");

        // // The first "match-start" should be ignored
        matches.pipeInput(
            createKeyFromJson(matchStartJson),
            matchStartJson.encode()
        );

        matches.pipeInput(
            createKeyFromJson(matchJsonA),
            matchJsonA.encode()
        );
        matches.pipeInput(
            createKeyFromJson(matchJsonB1),
            matchJsonB1.encode()
        );
        matches.pipeInput(
            createKeyFromJson(matchJsonB2),
            matchJsonB2.encode()
        );
        matches.pipeInput(
            createKeyFromJson(matchJsonC),
            matchJsonC.encode()
        );

        TestRecord<String, String> rA = aggregates.readRecord();
        // Read the intermediate messages for B, but we don't need
        // to run assertions against them since we care about the
        // end result only
        aggregates.readRecord();
        aggregates.readRecord();
        TestRecord<String, String> rC = aggregates.readRecord();

        JsonObject rAJSON = new JsonObject(rA.getValue());
        JsonObject rCJSON = new JsonObject(rC.getValue());

        // Check that the initial aggregate is created correctly
        Assertions.assertEquals(
            rA.getKey(),
            "254f961fc02aafdd:W4eXIr-t2EpVSgvuUHRsL"
        );
        Assertions.assertEquals(
            rAJSON.getString("gameId"),
            "254f961fc02aafdd"
        );
        Assertions.assertEquals(
            rAJSON.getString("playerId"),
            "W4eXIr-t2EpVSgvuUHRsL"
        );
        Assertions.assertEquals(
            rAJSON.getJsonArray("matches").encode(),
            new JsonArray().add("A").encode()
        );

        // Verify that updates to the aggregate work as expected
        Assertions.assertEquals(
            rC.getKey(),
            "254f961fc02aafdd:W4eXIr-t2EpVSgvuUHRsL"
        );
        Assertions.assertEquals(
            rCJSON.getString("gameId"),
            "254f961fc02aafdd"
        );
        Assertions.assertEquals(
            rCJSON.getString("playerId"),
            "W4eXIr-t2EpVSgvuUHRsL"
        );
        Assertions.assertEquals(
            rCJSON.getJsonArray("matches").encode(),
            new JsonArray().add("A").add("B").add("C").encode()
        );
    }

    private static String readFileAsString(String file)throws Exception {
        return new String(Files.readAllBytes(Paths.get(file)));
    }

    private String createKeyFromJson (JsonObject json) {
        return json.getJsonObject("data").getString("game") + ":" + json.getJsonObject("data").getString("match");
    }
}
