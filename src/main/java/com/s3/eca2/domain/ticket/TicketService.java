package com.s3.eca2.domain.ticket;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Service
public class TicketService {
    private final TicketRepository ticketRepository;

    public TicketService(TicketRepository ticketRepository) {
        this.ticketRepository = ticketRepository;
    }

    public Ticket find(long entityId) {
        return ticketRepository.findById(entityId)
                .orElseThrow(() -> new EntityNotFoundException("Ticket not found for id: " + entityId));
    }

    // TicketService.java
    public Page<Ticket> findTicketsByDate(Date start, Date end, Pageable pageable) {
        return ticketRepository.findByRegDateBetweenOrModDateBetween(start, end, pageable);
    }
}
