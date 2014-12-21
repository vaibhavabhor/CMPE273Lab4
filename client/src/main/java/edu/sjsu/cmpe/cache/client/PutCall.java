package edu.sjsu.cmpe.cache.client;

import java.util.concurrent.Callable;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

import javax.security.auth.callback.Callback;

/**
 * Created by vaibhav on 12/20/14.
 */

class PutCall implements Callable<HttpResponse<JsonNode>> {
    private final String serverUrl;
    private final String key;
    private final String value;
    private final Callback callback;
    public PutCall(String serverUrl,String key,String value,Callback callback){
        this.serverUrl = serverUrl;
        this.key = key;
        this.value = value;
        this.callback = callback;
    }
    @Override
    public HttpResponse<JsonNode> call() throws Exception {
        System.out.println(String.format("Trying to put %s => %s in node %s",key,value,this.serverUrl));
        return (HttpResponse<JsonNode>) Unirest.put(this.serverUrl + "/cache/{key}/{value}")
                .header("accept", "application/json")
                .routeParam("key", key)
                .routeParam("value", value)
                .asJsonAsync(callback).get();
    }
}