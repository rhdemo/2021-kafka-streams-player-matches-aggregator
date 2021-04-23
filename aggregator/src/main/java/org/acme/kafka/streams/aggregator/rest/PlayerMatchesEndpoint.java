package org.acme.kafka.streams.aggregator.rest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
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
import org.acme.kafka.streams.aggregator.streams.PipelineMetadata;
import org.jboss.logging.Logger;

import io.vertx.core.json.JsonObject;

import org.acme.kafka.streams.aggregator.streams.InteractiveQueries;

@ApplicationScoped
@Path("/game")
public class PlayerMatchesEndpoint {

    private static final Logger LOG = Logger.getLogger(TopologyProducer.class);

    @Inject
    InteractiveQueries interactiveQueries;

    @GET
    @Path("/{gameId}/player-matches/{playerId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPlayerMatches(@PathParam("gameId") String gameId, @PathParam("playerId") String playerId) {
        String id = gameId + ":" + playerId;

        LOG.debug("received get for key/id: " + id);

        QueryResult result = interactiveQueries.getPlayerMatchesStore(id);
        if (result.getResult().isPresent()) {
            LOG.debug("get for key/id was found in local store: " + id);
            return Response.ok(result.getResult().get()).build();
        } else if (result.getHost().isPresent()) {
            URL url = getOtherUrl(result.getHost().get(), result.getPort().getAsInt(), gameId, playerId);
            LOG.debug("get for key/id was found in node at URL: " + url.toString());
            try {
                HttpURLConnection conn = (HttpURLConnection)url.openConnection();
                conn.setRequestMethod("GET");
                conn.setDoOutput(true);
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);

                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();

                return Response.ok(content).build();
            } catch (Exception e) {
                LOG.error("error fetching: " + url.toString());
                LOG.error("error details: " + e.toString());
                return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode(), "{ \"info\": \"Error fetching data from other Kafka Streams node\" }").build();
            }
        } else {
            return Response.status(Status.NOT_FOUND.getStatusCode(), "No data found for weather station " + id).build();
        }
    }

    @GET
    @Path("/meta-data")
    @Produces(MediaType.APPLICATION_JSON)
    public List<PipelineMetadata> getMetaData() {
        return interactiveQueries.getMetaData();
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

    private URL getOtherUrl(String host, int port, String gameId, String playerId) {
        try {
            return new URL("http://" + host + ":" + port + "/game/" + gameId + "/player-matches/" + playerId);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
