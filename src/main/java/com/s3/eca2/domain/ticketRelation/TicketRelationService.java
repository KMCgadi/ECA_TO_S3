package com.s3.eca2.domain.ticketRelation;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;

@Service
public class TicketRelationService {

    private final TicketRelationRepository ticketRelationRepository;

    public TicketRelationService(TicketRelationRepository ticketRelationRepository){
        this.ticketRelationRepository = ticketRelationRepository;
    }
    public TicketRelation find(long ticketRelationEid) {
        return ticketRelationRepository.findById(ticketRelationEid)
                .orElseThrow(() -> new EntityNotFoundException("TicketRelation not found for id :" + ticketRelationEid));
    }
    public Page<TicketRelation> findTicketRelationByDate(Date start, Date end, Pageable pageable) {
        return ticketRelationRepository.findByRegDateBetweenOrModDateBetween(start, end, pageable);
    }
}
