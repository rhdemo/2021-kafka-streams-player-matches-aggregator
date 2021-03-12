package org.acme.kafka.streams.aggregator.rest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.acme.kafka.streams.aggregator.streams.QueryResult;
import org.acme.kafka.streams.aggregator.streams.TopologyProducer;
import org.apache.kafka.streams.Topology;

import io.vertx.core.json.JsonObject;

import org.acme.kafka.streams.aggregator.streams.InteractiveQueries;
import org.acme.kafka.streams.aggregator.streams.PipelineMetadata;

@ApplicationScoped
@Path("/game")
public class PlayerMatchesEndpoint {

    @Inject
    InteractiveQueries interactiveQueries;

    @GET
    @Path("/{gameId}/player-matches/{playerId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPlayerMatches(@PathParam("gameId") String gameId, @PathParam("playerId") String playerId) {
        String id = gameId + ":" + playerId;

        QueryResult result = interactiveQueries.getPlayerMatchesStore(id);
        if (result.getResult().isPresent()) {
            return Response.ok(result.getResult().get()).build();
        } else if (result.getHost().isPresent()) {
            URI otherUri = getOtherUri(result.getHost().get(), result.getPort().getAsInt(), id);
            return Response.seeOther(otherUri).build();
        } else {
            return Response.status(Status.NOT_FOUND.getStatusCode(), "No data found for weather station " + id).build();
        }
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCurrentGameId () {
        String gameId = TopologyProducer.latestGameId;
        JsonObject json = new JsonObject();

        json.put("gameId", gameId);

        return Response.ok(json).build();
    }

    private URI getOtherUri(String host, int port, String id) {
        try {
            return new URI("http://" + host + ":" + port + "/weather-stations/data/" + id);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
