package safaricomet.bigdata.nifi.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Service;
;
import safaricomet.bigdata.nifi.config.RestTemplateConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Service
@Slf4j
public class ListSftp {
    ObjectMapper objectMapper = new ObjectMapper();
    @Autowired
    RestTemplateConf restTemplate;



    public  Object updateListSftpAndFetchSftp(String rootId){
        try{
            HashMap map= (HashMap) this.getProcessGroups(rootId);

            List<Map> list = (List<Map>) map.get("processGroups");
            list.parallelStream().forEach(group->{

                Map component = (Map) group.get("component");
                String id =group.get("id").toString();
                String name = (String) component.get("name");
                if (name.equals("COLLAB") || name.equals("NCC")){
                      this.updateProcessGroup2(id);
                }else {
                    try {
                        this.updateProcessGroup3(id);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            return list;
        }catch (Exception e){
            e.printStackTrace();
            return  new ArrayList<>();
        }
    }


    public  Object getProcessGroups(String id) throws IOException {
        String apiEndpoint =  "/process-groups/"+id +"/process-groups";
       return restTemplate.get(apiEndpoint);
    }

    public  Object getProcessors(String id) {

            String apiEndpoint =  "/process-groups/"+ id +"/processors";
        try {
            return restTemplate.get(apiEndpoint);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    public  void updateProcessor(String id, Object body){
            String endpoint = "/processors/" + id;
            restTemplate.putReq(endpoint,body);

    }

    public  Object updateProcessGroup2(String id)  {
        try {
            HashMap map= (HashMap) this.getProcessGroups(id);
//        List<Object> processors = new ArrayList<>();
            List<Map> list = (List<Map>) map.get("processGroups");

            list.parallelStream().forEach(processGroup->{
                HashMap myProcessors= (HashMap) this.getProcessors(processGroup.get("id").toString());

                List<Map> processors= (List<Map>) myProcessors.get("processors");
                processors.parallelStream().forEach(processor ->{
                    Map map1 = (Map) processor.get("component");
                    if (map1.get("name").equals("ListSFTP")) {
                        map1.put("config", getListSFTPReplacementObject((HashMap) map1.get("config")));
                        processor.put("component",map1);
                        this.updateProcessor((String) map1.get("id"),processor);
                    } else if (map1.get("name").equals("FetchSFTP")) {
                        HashMap conf = (HashMap) getFetchSFTPReplacementObject((HashMap) map1.get("config"));

                        map1.put("config", conf);
                        processor.put("component",map1);
                        log.info(processor.toString());
                        this.updateProcessor((String) map1.get("id"),processor);
                    }
                });

            });
            return  null;
        }catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }

    public  void updateProcessGroup3(String id) throws IOException {
        HashMap map= (HashMap) this.getProcessGroups(id);


        List<Map> list = (List<Map>) map.get("processGroups");

        for (Map group : list){
            this.updateProcessGroup2(group.get("id").toString());
        }
    }

    public  Object getListSFTPReplacementObject(HashMap object){
        HashMap map = (HashMap) object.get("properties");
        String path = (String) map.get("Remote Path");
        log.info("Path: {}",path);
        map.put("Remote Path",path.replaceAll("/cdrs","#{primary}"));
        map.put("Hostname","#{host}");
        map.put("Username","#{username}");
        map.put("Password","#{password}");
        object.put("properties",map);
        return  object;
    }

    public  Object getFetchSFTPReplacementObject(HashMap object){
        HashMap map = (HashMap) object.get("properties");
        String path = (String) map.get("Move Destination Directory");
        log.info("Backup Path: {}",path);

        map.put("Move Destination Directory",path.replaceAll("/backupcdrs","#{secondary}"));

        map.put("Remote File","${path}/${filename}");
        map.put("Hostname","#{host}");
        map.put("Username","#{username}");
        map.put("Password","#{password}");
        object.put("properties",map);

        return  object;
    }

}
