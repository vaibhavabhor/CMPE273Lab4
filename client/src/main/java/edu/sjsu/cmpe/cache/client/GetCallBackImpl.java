package edu.sjsu.cmpe.cache.client;

import java.util.Map;
import java.util.concurrent.Callable;
import com.mashape.unirest.http.JsonNode;

/**
 * Created by vaibhav on 12/20/14.
 */
public class GetCallBackImpl implements Callback<JsonNode> {
    private final String serverUrl;
    private final Map<String, String> statusMap;
    public GetCallBackImpl(String serverUrl,Map<String,String> statusMap){
        this.serverUrl = serverUrl;
        this.statusMap = statusMap;
    }
    @Override
    public void completed(HttpResponse<JsonNode> response) {
        System.out.println("Completed call for node: "+this.serverUrl);
        if(response.getStatus()==200){
            JsonNode body = response.getBody();
            String value = body.getObject().getString("value");
            statusMap.put(serverUrl, value);
            System.out.println(String.format("Get call for Node %s and obtained value %s", serverUrl,value));
        } else{
            System.out.println(String.format("Get call for Node %s and did not obtain value", serverUrl));
            statusMap.put(serverUrl,null);
        }
    }
    @Override
    public void failed(UnirestException e) {
        System.out.println(String.format("Failed with exception %s",e.getMessage()));
    }
    @Override
    public void cancelled() {
        System.out.println(String.format("Cancelled request to node %s",serverUrl));
    }
}
