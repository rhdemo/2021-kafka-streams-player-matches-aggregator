package org.acme.kafka.streams.aggregator.streams;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;


/**
 * Integration testing of the application with an embedded broker.
 */
@QuarkusTest
@QuarkusTestResource(KafkaResource.class)
public class AggregatorTest {

    String matchStartJsonString;
    String attackJsonString;
    String matchEndJsonString;

    @BeforeEach
    public void setUp() throws Exception {
        matchStartJsonString = readFileAsString("src/test/resources/match-start.json");
        attackJsonString = readFileAsString("src/test/resources/attack.json");
        matchEndJsonString = readFileAsString("src/test/resources/match-end.json");
    }

    @AfterEach
    public void tearDown() {

    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testMatchStartAggregation() throws InterruptedException {
    }

    private static String readFileAsString(String file)throws Exception {
        return new String(Files.readAllBytes(Paths.get(file)));
    }
}
