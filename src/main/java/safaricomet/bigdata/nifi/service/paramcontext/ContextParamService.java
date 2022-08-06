package safaricomet.bigdata.nifi.service.paramcontext;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import safaricomet.bigdata.nifi.config.RestTemplateConf;


import java.util.HashMap;

import java.util.Objects;

@Service
@Slf4j
public class ContextParamService {


    @Autowired
    RestTemplateConf restTemplateConf;

    private  String primary="/cdrs";
    private  String secondary="/backupcdrs";
    private  String backup = "/archive";
    private  String template ="{\n" +
            "  \"revision\": {\n" +
            "    \"version\": %s\n" +
            "  },\n" +
            "  \"id\": \"%s\",\n" +
            "  \"component\": {\n" +
            "    \"parameters\": [\n" +
            "      {\n" +
            "        \"parameter\": {\n" +
            "          \"name\": \"secondary\",\n" +
            "          \"value\": \"%s\"\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"parameter\": {\n" +
            "          \"name\": \"primary\",\n" +
            "          \"value\": \"%s\"\n" +
            "        }\n" +
            "      }\n" +
            "    ],\n" +
            "    \"id\": \"%s\"\n" +
            "  }\n" +
            "}";
    public Object updateParamContext(String state,String id) throws JsonProcessingException {
        String body=null;
        String version=this.getParamContextPrevVersion(id);
        log.info("version: {}, state: {}",version,state);
        if(Objects.equals(state, "primary")){
            body = String.format(template,version,id,secondary,primary,id);
        }else {
            body = String.format(template,version,id,backup,secondary,id);
        }
       return restTemplateConf.putReq("/parameter-contexts/"+id,body);
    }

    public  String getParamContextPrevVersion(String id) throws JsonProcessingException {
        HashMap obj = (HashMap) restTemplateConf.get("/parameter-contexts/"+id);
        HashMap<String,String> revision = (HashMap) obj.get("revision");
        return String.valueOf(revision.get("version"));
    }

}
