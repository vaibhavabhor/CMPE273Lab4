package edu.sjsu.cmpe.cache.client;

import java.util.concurrent.Callable;

/**
 * Created by vaibhav on 12/20/14.
 */
public class GetCall implements Callable<String> {
    @Override
    public String call() throws Exception {
        HttpResponse<JsonNode> response = null;
        try {
            response = Unirest.get(this.cacheServerUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key)).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }
        String value = response.getBody().getObject().getString("value");

        return value;
    }
    }
