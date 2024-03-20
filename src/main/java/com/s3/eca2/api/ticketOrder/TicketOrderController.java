package com.s3.eca2.api.ticketOrder;

import com.s3.eca2.domain.ticketOrder.TicketOrder;
import com.s3.eca2.domain.ticketOrder.TicketOrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/rest/api/v1/s3/ticketOrder")
public class TicketOrderController {

    private final TicketOrderService ticketOrderService;

    public TicketOrderController(TicketOrderService ticketOrderService){
        this.ticketOrderService = ticketOrderService;
    }

    @GetMapping("/{ticketOrderEid}")
    public TicketOrder selectOne(@PathVariable long ticketOrderEid){
        return ticketOrderService.find(ticketOrderEid);
    }
}
