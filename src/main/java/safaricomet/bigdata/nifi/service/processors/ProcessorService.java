package safaricomet.bigdata.nifi.service.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import safaricomet.bigdata.nifi.config.RestTemplateConf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Service
@Slf4j
public class ProcessorService {

    @Autowired
    ProcessGroupService processGroupService;
    @Autowired
    RestTemplateConf restTemplateConf;

    String updateBody ="{\n" +
            "  \"revision\": {\n" +
            "    \"version\": \"%s\"\n" +
            "  },\n" +
            "  \"state\": \"%s\",\n" +
            "  \"disconnectedNodeAcknowledged\": true\n" +
            "}";

    public Object stopProcessors(String type,String id){
        try {
            List<Map> list = new ArrayList<>();
            list = processGroupService.getProcessorIdAndVersion(type,id);
            list.parallelStream().forEach(each ->{
//                log.info("id: {}, Version: {}",each.get("id"), each.get("version"));
                this.updateProcessor((String) each.get("id"), (String) each.get("version"),"STOPPED");
            });
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return  false;
    }
    public Object startProcessors(String type,String id){
        try {
            List<Map> list = processGroupService.getProcessorIdAndVersion(type,id);
            list.parallelStream().forEach(each ->{
                this.updateProcessor((String) each.get("id"), (String) each.get("version"),"RUNNING");
            });
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return  false;
    }

    public  void  updateProcessor(String id, String revision,String state){
        String endpoint =  "/processors/" + id + "/run-status";

        String newBody=String.format(updateBody, revision,state);
        restTemplateConf.putReq(endpoint,newBody);
    }


}
