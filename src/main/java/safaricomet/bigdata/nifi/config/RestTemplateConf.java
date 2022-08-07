package safaricomet.bigdata.nifi.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import javax.jms.Session;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@EnableScheduling
public  class RestTemplateConf {
    ObjectMapper objectMapper = new ObjectMapper();
    @Value("${nifi.primary.server}")
    String resourceUrl ;

    @Value("${nifi.primary.password}")
    String password ;

    @Value("${nifi.primary.username}")
    String username ;
    @Autowired
    RestTemplate restTemplate;

    volatile String bearerToken;

    public String putReq(String url, Object body){
        try {
            String apiEndpoint = resourceUrl + url;
            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(this.getBearerToken1());
            HttpEntity<Object> requestEntity = new HttpEntity<>(body, headers);
            log.info("URL: {}",apiEndpoint);

            ResponseEntity<String> response= restTemplate.exchange(apiEndpoint,HttpMethod.PUT,requestEntity,String.class);
            log.info("Status: {}",response.getHeaders());
            log.info("Body:\n{}",response.getBody());
            if (response.getStatusCodeValue() != 200){
                throw  new RuntimeException(String.valueOf(response.getStatusCode()));
            }
            return  response.getBody();

        }catch (Exception e){
            e.printStackTrace();
            return  e.getMessage();
        }
    }



    public  Object get(String url) throws JsonProcessingException {
        try {
            String apiEndpoint = resourceUrl + url;
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.ACCEPT,"*/*");
            headers.setBearerAuth(this.getBearerToken1());
            HttpEntity<Object> requestEntity = new HttpEntity<>(headers);
            ResponseEntity<String> response = restTemplate.exchange(apiEndpoint,HttpMethod.GET,requestEntity,String.class);
            log.info("Status: {}",response.getHeaders());
            log.info("Body:\n{}",response.getBody());
            if (response.getStatusCodeValue() != 200){
                throw  new RuntimeException(String.valueOf(response.getStatusCode()));
            }
            return objectMapper.readValue(response.getBody(), Map.class);
        }catch (Exception e){
            e.printStackTrace();
            return  e.getMessage();
        }

    }

    @Scheduled(fixedDelay = 1000 * 60 * 60 * 12, initialDelay = 0)
    public  void getBearerToken(){
        String apiEndpoint = resourceUrl + "/access/token";
        HttpHeaders headers = new HttpHeaders();

        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        String body= String.format("username=%s&password=%s",username,password );
        HttpEntity<Object> requestEntity = new HttpEntity<>(body, headers);
        log.info("Req headers: {}",requestEntity.getHeaders());
        HttpEntity<String> response= restTemplate.exchange(apiEndpoint, HttpMethod.POST, requestEntity,String.class);
        this.bearerToken =response.getBody();
    }


    public  String getBearerToken1(){
        String apiEndpoint = resourceUrl + "/access/token";
        HttpHeaders headers = new HttpHeaders();

        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        String body= String.format("username=%s&password=%s",username,password );
        HttpEntity<Object> requestEntity = new HttpEntity<>(body, headers);
//        log.info("Req headers: {}",requestEntity.getHeaders());
        ResponseEntity<String> response= restTemplate.exchange(apiEndpoint, HttpMethod.POST, requestEntity,String.class);
        if (response.getStatusCodeValue() == 201){
            return response.getBody();
        }else {
            return  getBearerToken1();
        }

    }
}
