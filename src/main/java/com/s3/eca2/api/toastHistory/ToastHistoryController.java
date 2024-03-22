package com.s3.eca2.api.toastHistory;

import com.s3.eca2.domain.toastHistory.ToastHistory;
import com.s3.eca2.domain.toastHistory.ToastHistoryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/rest/api/v1/s3/toasthistory")
public class ToastHistoryController {

    private final ToastHistoryService toastHistoryService;

    public ToastHistoryController(ToastHistoryService toastHistoryService){
        this.toastHistoryService = toastHistoryService;
    }

    @GetMapping("/{entityId}")
    public ToastHistory selectOne(@PathVariable long entityId){
        return toastHistoryService.find(entityId);
    }

}
