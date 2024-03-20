package com.s3.eca2.domain.ticketChannel;

import com.s3.eca2.domain.surveyResult.SurveyResult;
import com.s3.eca2.domain.surveyResult.SurveyResultRepository;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;
import java.util.List;

@Service
public class TicketChannelService {

    private final TicketChannelRepository ticketChannelRepository;

    public TicketChannelService(TicketChannelRepository ticketChannelRepository){
        this.ticketChannelRepository = ticketChannelRepository;
    }

    public TicketChannel find(long ticketChannelEid){
        return ticketChannelRepository.findById(ticketChannelEid)
                .orElseThrow(() -> new EntityNotFoundException("TicketChannel not found for id :" + ticketChannelEid));
    }
    public List<TicketChannel> findTicketChannelByDate(Date start, Date end) {
        return ticketChannelRepository.findByRegDateBetweenOrModDateBetween(start, end);
    }
}
