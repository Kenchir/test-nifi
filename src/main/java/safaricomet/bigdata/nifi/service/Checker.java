package safaricomet.bigdata.nifi.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import safaricomet.bigdata.nifi.config.RestTemplateConf;

@Slf4j
@Service
public class Checker {

    @Autowired
    RestTemplateConf restTemplate;


    public Object isPrimaryRunning() throws JsonProcessingException {
        String token = restTemplate.getBearerToken1();
       Object responseEntity = restTemplate.get("/system-diagnostics",token);
//        log.info("Status: {}",responseEntity.getStatusCodeValue());
        return  responseEntity;

    }
}
