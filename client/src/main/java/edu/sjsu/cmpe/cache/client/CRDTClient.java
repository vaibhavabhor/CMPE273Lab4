package edu.sjsu.cmpe.cache.client;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by vaibhav on 12/20/14.
 */
public class CRDTClient {
        private final List<String> servers;
        private final DistributedCacheService distributedCacheService;

        public CRDTClient(List<String> servers) {
                this.servers = servers;
                this.distributedCacheService = new DistributedCacheService(null, servers);
        }

        public void put(int key, String value) {
                Map<String, Boolean> writeStatus = distributedCacheService.asyncPut(key, value);
                if(!hasSufficientSuccessRate(writeStatus)){
                        distributedCacheService.rollbackWrite(key,writeStatus);
                }
        }

        private boolean hasSufficientSuccessRate(Map<String, Boolean> statusMap) {
                int successFullWrites = 0;
                for(Map.Entry<String, Boolean> statusEntry:statusMap.entrySet()){
                        if(statusEntry.getValue()){
                                successFullWrites+=1;
                        }
                }
                if (successFullWrites >=2)
                        return true;
                else return false;
        }

        public void get(int key) {
                final Map<String, String> serverAndValues = distributedCacheService.asyncGet(key);
                HashMap<String, Integer> alphabetAndCount = new HashMap<String, Integer>(3);
                for(Map.Entry<String,String> entry: serverAndValues.entrySet()){
                        String serverUrl = entry.getKey();
                        String alphabet = entry.getValue();
                        if(alphabetAndCount.containsKey(alphabet) && alphabet!=null){
                                int value = alphabetAndCount.get(alphabet);
                                alphabetAndCount.put(alphabet, value + 1);
                        }else{
                                alphabetAndCount.put(alphabet, 1);
                        }
                }
                Integer max = null;
                if (alphabetAndCount.size()>0){
                        max = Collections.max(alphabetAndCount.values());
                }
                String majorityValue = null;
                for(Map.Entry entry :alphabetAndCount.entrySet()) {
                        if(entry.getValue() == max){
                                majorityValue = (String) entry.getKey();
                        }
                }
                for(Map.Entry<String,String> entry: serverAndValues.entrySet()){
                        if(entry.getValue()==null){
                                System.out.println(String.format("Now Read repairing the Key %s for node %s ",key,entry.getKey()));
                                String nodeToBeRepaired = entry.getKey();
                                DistributedCacheService repairDistributed = new DistributedCacheService(nodeToBeRepaired, null);
                                System.out.println("majorityValue is: "+majorityValue);
                                repairDistributed.put(key,majorityValue);
                        }
                }
        }
}

