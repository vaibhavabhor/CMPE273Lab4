package edu.sjsu.cmpe.cache.client;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Created by vaibhav on 12/20/14.
 */

class PutCallBackImpl implements Callback<JsonNode> {
    private final String serverUrl;
    private final Map<String, Boolean> statusMap;
    public PutCallBackImpl(String serverUrl, Map<String, Boolean> statusMap){
        this.serverUrl = serverUrl;
        this.statusMap = statusMap;
    }
    public void failed(UnirestException e) {
        System.out.println("Request Failed to node with url "+ serverUrl);
        statusMap.put(serverUrl, false);
    }
    public void completed(HttpResponse<JsonNode> response) {
        if(response.getStatus()==200){
            System.out.println("Request Succeeded for node "+ serverUrl);
            statusMap.put(serverUrl,true);
        }else{
            statusMap.put(serverUrl,false);
        }
    }
    public void cancelled() {
        System.out.println("The request has been cancelled");
    }
}