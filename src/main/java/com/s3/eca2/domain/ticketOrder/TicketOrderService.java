package com.s3.eca2.domain.ticketOrder;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;

@Service
public class TicketOrderService {

    private final TicketOrderRepository ticketOrderRepository;

    public TicketOrderService(TicketOrderRepository ticketOrderRepository){
        this.ticketOrderRepository = ticketOrderRepository;
    }

    public TicketOrder find(long ticketOrderEid){
        return ticketOrderRepository.findById(ticketOrderEid)
                .orElseThrow(() -> new EntityNotFoundException("TicketOrder not found for id: " + ticketOrderEid));
    }
    public Page<TicketOrder> findTicketOrderByDate(Date start, Date end, Pageable pageable) {
        return ticketOrderRepository.findByRegDateBetweenOrModDateBetween(start, end, pageable);
    }
}
