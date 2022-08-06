package safaricomet.bigdata.nifi.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import safaricomet.bigdata.nifi.service.Checker;
import safaricomet.bigdata.nifi.service.ListSftp;
import safaricomet.bigdata.nifi.service.processors.ProcessorService;

@RestController
@RequestMapping("/api/v1")
@Slf4j
public class Processor {

    @Autowired
    ListSftp listSftp;

    @Autowired
    Checker checker;

    @Autowired
    ProcessorService processorService;


    @GetMapping("/stop-processors/{id}")
    public ResponseEntity<Object> stopProcessor(@RequestParam(required = true) String type,@PathVariable(required = true) String id){
        try {
            return  new ResponseEntity<>(processorService.stopProcessors(type,id), HttpStatus.OK);
        }catch (Exception e){
            e.printStackTrace();
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/start-processors/{id}")
    public ResponseEntity<Object> startProcessor(@RequestParam(required = true) String type,@PathVariable(required = true) String id){
        try {
            return  new ResponseEntity<>(processorService.startProcessors(type,id), HttpStatus.OK);
        }catch (Exception e){
            e.printStackTrace();
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }
}
