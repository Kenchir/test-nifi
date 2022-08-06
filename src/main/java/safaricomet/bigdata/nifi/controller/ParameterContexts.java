package safaricomet.bigdata.nifi.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import safaricomet.bigdata.nifi.service.paramcontext.ContextParamService;

@RestController
@RequestMapping("/api/v1")
@Slf4j
public class ParameterContexts {

    @Autowired
    ContextParamService contextParamService;

    @GetMapping("/param-contexts")
    public ResponseEntity<Object> updatePrimary(@RequestParam(required = true) String id,@RequestParam(required = true) String state){
        try {
            return  new ResponseEntity<>(contextParamService.updateParamContext(state,id),HttpStatus.OK);
        }catch (Exception e){
           e.printStackTrace();
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
