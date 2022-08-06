package safaricomet.bigdata.nifi.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import safaricomet.bigdata.nifi.service.Checker;
import safaricomet.bigdata.nifi.service.ListSftp;
import safaricomet.bigdata.nifi.service.processors.ProcessGroupService;
import safaricomet.bigdata.nifi.service.processors.ProcessorService;

@RestController
@RequestMapping("/api/v1/process-group")
@Slf4j
public class ProcessGroup {
    @Autowired
    ListSftp listSftp;

    @Autowired
    Checker checker;

    @Autowired
    ProcessGroupService processGroupService;

    @GetMapping("/updateListAndFetch/{id}")
    public Object updateListAndFetch(@PathVariable(required = true) String id){
        return  listSftp.updateListSftpAndFetchSftp(id);
    }

    @GetMapping("/status")
    public Object status() throws JsonProcessingException {
        return  checker.isPrimaryRunning();
    }

    @GetMapping("/list-fetch-queue-count/{id}")
    public ResponseEntity<Object> getQueueCount(@PathVariable(required = true) String id){
        try {
            return  new ResponseEntity<>(processGroupService.getQueueCount(id), HttpStatus.OK);
        }catch (Exception e){
            e.printStackTrace();
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
