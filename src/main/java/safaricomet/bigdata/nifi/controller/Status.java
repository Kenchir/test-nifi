package safaricomet.bigdata.nifi.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.RestTemplate;
import safaricomet.bigdata.nifi.config.RestTemplateConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
@Slf4j
public class Status {

   private String paramContext =" {\n" +
            "      \"id\": \"%s\",\n" +
            "      \"permissions\": {\n" +
            "        \"canRead\": true,\n" +
            "        \"canWrite\": true\n" +
            "      },\n" +
            "      \"component\": {\n" +
            "        \"id\": \"%s\",\n" +
            "        \"name\": \"sftp\"\n" +
            "      }\n" +
            "    }";

    private final List<String> group2 = new ArrayList<String>();

    public  Status() throws IOException {
        this.group2.add("NCC");
        this.group2.add("COLLAB");
    }

    ObjectMapper objectMapper = new ObjectMapper();


    @Autowired
    RestTemplateConf restTemplate;

   private HashMap paramContext1 ;



    @GetMapping("/add_param_contexts/{id}")
    public Object updateGroups(@PathVariable(required = true) String id,@RequestParam(required = true) String paramId){

        try{
            String param = String.format(this.paramContext,paramId,paramId);
            this.paramContext1 = (HashMap) objectMapper.readValue(param, HashMap.class);;
            HashMap map= (HashMap) this.getProcessGroupProcessors(id);

            List<Map> list = (List<Map>) map.get("processGroups");
            for (Map group : list){

                Map component = (Map) group.get("component");
                String groupId =group.get("id").toString();
                String name = (String) component.get("name");
                //Process 6D and COLLAB
                if (name.equals("COLLAB") || name.equals("NCC")){
                      this.updateProcessGroup2(groupId);
                }else {
                    this.updateProcessGroup3(groupId);
                }
            }
            return list;
        }catch (Exception e){
            e.printStackTrace();
            return  new ArrayList<>();
        }

    }
    public  Object getProcessGroupProcessors(String id) throws IOException {
        String apiEndpoint =  "/process-groups/"+id +"/process-groups";
      return  restTemplate.get(apiEndpoint);
    }

    public  Object updateProcessGroup2(String id) throws IOException {
        HashMap map= (HashMap) this.getProcessGroupProcessors(id);
        List<Map> list = (List<Map>) map.get("processGroups");

        list.forEach(map1 ->{
             HashMap component = (HashMap) map1.get("component");
             component.put("parameterContext",this.paramContext1);
             map1.put("component",component);
            Object a = this.putRequest(map1.get("id").toString(),map1);
            log.debug(a.toString());
        });
        return  list;
    }
    public  void updateProcessGroup3(String id) throws IOException {
        HashMap map= (HashMap) this.getProcessGroupProcessors(id);


        List<Map> list = (List<Map>) map.get("processGroups");
        for (Map group : list){
//            log.info("{}",group);
            String idN =group.get("id").toString();
            this.updateProcessGroup2(idN);
        }
    }

    public  Object putRequest(String id, Object body){
        String endpoint ="/process-groups/" + id;
            return restTemplate.putReq(endpoint,body);
    }

}
