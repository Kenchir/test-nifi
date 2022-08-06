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
import org.springframework.web.client.RestTemplate;

import javax.jms.Session;
import java.util.Arrays;
import java.util.HashMap;

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

    private  String cookie;
    private String bearerToken;


    public String putReq(String url, Object body){
        try {

//            log.info("Body {}",body);
            url = resourceUrl +url;
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.AUTHORIZATION, this.bearerToken);
            headers.add("Set-Cookie",this.cookie);
            headers.setOrigin("127.0.0.1");
            headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
            headers.setContentType(MediaType.APPLICATION_JSON);
//            headers.add("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36");

            ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
            HttpEntity<Object> requestEntity = new HttpEntity<Object>(body, headers);
//            log.info("Req entity: {}",requestEntity);
             restTemplate.put(url,  requestEntity);
//            return  response.getBody();
            return  "test";
        }catch (Exception e){

//            log.error("Error: {}",e.getMessage());
            e.printStackTrace();
            return  e.getMessage();
        }
    }


    public  Object get(String url) throws JsonProcessingException {
        String apiEndpoint = resourceUrl + url;
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.AUTHORIZATION,this.bearerToken);
        HttpEntity<Object> requestEntity = new HttpEntity<>(headers);
        ResponseEntity<String> response = restTemplate.exchange(apiEndpoint,HttpMethod.GET,requestEntity,String.class);
        return objectMapper.readValue(response.getBody(), HashMap.class);
    }

    @Scheduled(fixedDelay = 1000 * 60 * 60 * 12, initialDelay = 0)
    public  void getBearerToken(){
        log.info("username: {}, password: {}",username,password);
        String apiEndpoint = resourceUrl + "/access/token";
        HttpHeaders headers = new HttpHeaders();

        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        String body= String.format("username=%s&password=%s",username,password );
        log.info("body:{}",body);
        HttpEntity<Object> requestEntity = new HttpEntity<>(body, headers);
        log.info("Req headers: {}",requestEntity.getHeaders());
        HttpEntity<String> response= restTemplate.exchange(apiEndpoint, HttpMethod.POST, requestEntity,String.class);
        this.bearerToken ="Bearer "+ response.getBody();

        log.info(" Bearer Token: {}", bearerToken);
        log.info("Cookie :{}",response.getHeaders());
//        this.cookie=response.getHeaders().get("Set-Cookie");
    }
}
