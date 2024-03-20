package com.s3.eca2.api.ticketRelation;

import com.s3.eca2.domain.ticketRelation.TicketRelation;
import com.s3.eca2.domain.ticketRelation.TicketRelationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/rest/api/v1/s3/ticketRelation")
public class TicketRelationController {

    private final TicketRelationService ticketRelationService;

    public TicketRelationController(TicketRelationService ticketRelationService){
        this.ticketRelationService = ticketRelationService;
    }
    @GetMapping("/{ticketRelationEid}")
    public TicketRelation selectOne(@PathVariable long ticketRelationEid){
        return ticketRelationService.find(ticketRelationEid);
    }
}
