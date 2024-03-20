package com.s3.eca2.domain.ticketRelation;

import com.s3.eca2.domain.ticketOrder.TicketOrder;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;
import java.util.List;

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
    public List<TicketRelation> findOrganizationTypeByDate(Date start, Date end) {
        return ticketRelationRepository.findByRegDateBetweenOrModDateBetween(start, end);
    }
}
