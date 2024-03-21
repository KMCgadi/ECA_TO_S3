package com.s3.eca2.domain.ticketOrder;

import com.s3.eca2.domain.ticketChannel.TicketChannel;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;
import java.util.List;

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
    public List<TicketOrder> findOrganizationTypeByDate(Date start, Date end) {
        return ticketOrderRepository.findByRegDateBetweenOrModDateBetween(start, end);
    }
}