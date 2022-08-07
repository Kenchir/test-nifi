package safaricomet.bigdata.nifi.service.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import safaricomet.bigdata.nifi.config.RestTemplateConf;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class ProcessGroupService {


    @Autowired
    RestTemplateConf restTemplateConf;

    public List<Map> getProcessorIdAndVersion(String type,String id) throws Exception {
        String token = restTemplateConf.getBearerToken1();
        List<Map> processorIds = new ArrayList<>();
        List<Map> processGroups = this.getProcessGroupsWithFetchAndList(id,token);

        processGroups.parallelStream().forEach(each -> {
            List<Map> proc = null;
            try {
                proc = this.getAllProcessorIds(each.get("id").toString(), type,token);

                processorIds.addAll(proc);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
        return processorIds;

    }

    public  Object getQueueCount(String id) throws  Exception{
        String token = restTemplateConf.getBearerToken1();
        HashMap<String, AtomicInteger> totalcount = new HashMap<>();
        AtomicInteger totalQueuedCount = new AtomicInteger();
        List<Map> processGroups = this.getProcessGroupsWithFetchAndList(id,token);

        processGroups.parallelStream().forEach(group->{
            HashMap status = (HashMap) group.get("status");
            HashMap<String,String> aggregateSnapshot = (HashMap) status.get("aggregateSnapshot");
//            log.info("name: {}, count: {}", status.get("name"),aggregateSnapshot.get("queuedCount"));
            totalQueuedCount.addAndGet(Integer.parseInt(aggregateSnapshot.get("queuedCount").replace(",","")));
        });
        totalcount.put("totalQueueCount",totalQueuedCount);
        return  totalcount;
    }

    public List<Map> getProcessGroupsWithFetchAndList(String id,String token) throws Exception {
        HashMap map = (HashMap) this.getProcessGroups(id,token);
        List<Map> processorGroups = new ArrayList<>();
        List<Map> list = (List<Map>) map.get("processGroups");

        list.parallelStream().forEach(group -> {
            Map component = (Map) group.get("component");
            String name = (String) component.get("name");
            if (name.equals("COLLAB") || name.equals("NCC") || name.equals("CONTROL_FILES")) {
                HashMap groups = null;
                try {
                    groups = (HashMap) this.getProcessGroups(group.get("id").toString(),token);
                    List<Map> processGroups = (List<Map>) groups.get("processGroups");
                    processorGroups.addAll(processGroups);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            } else {
                HashMap groups = null;
                try {
                    groups = (HashMap) this.getProcessGroups(group.get("id").toString(),token);
                    List<Map> processGroups = (List<Map>) groups.get("processGroups");
                    processGroups.parallelStream().forEach(each->{
                        HashMap groups1 = null;
                        try {
                            groups1 = (HashMap) this.getProcessGroups(each.get("id").toString(),token);
                            List<Map> processGroups1 = (List<Map>) groups1.get("processGroups");
                            processorGroups.addAll(processGroups1);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    });
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return processorGroups;
    }

    public List<Map> getAllProcessorIds(String id, String type,String token) throws JsonProcessingException {
        List<Map> processorIds = new ArrayList<>();
        HashMap processors = (HashMap) this.getProcessors(id,token);

        List<Map> list = (List<Map>) processors.get("processors");

        list.stream().parallel().forEach(each -> {
                Map map2 = (Map) each.get("component");
                if (map2.get("name").equals(type)) {
                    HashMap<String, String> map1 = new HashMap<>();
                    HashMap<String, Integer> revision = (HashMap<String, Integer>) each.get("revision");
                    map1.put("id", each.get("id").toString());
                    map1.put("version", String.valueOf(revision.get("version")));
                    processorIds.add(map1);
                }
            });
        return processorIds;
    }


    public Object getProcessors(String id,String token) throws JsonProcessingException {
        String apiEndpoint = "/process-groups/" + id + "/processors";
        return restTemplateConf.get(apiEndpoint,token);
    }

    public Object getProcessGroups(String id,String token) throws JsonProcessingException {
        String apiEndpoint = "/process-groups/" + id + "/process-groups";
        return restTemplateConf.get(apiEndpoint,token);
    }


}
