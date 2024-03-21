package com.s3.eca2.domain.ticketChannel;

import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;
import java.util.List;

@Service
public class ChannelService {

    private final ChannelRepository channelRepository;

    public ChannelService(ChannelRepository channelRepository){
        this.channelRepository = channelRepository;
    }

    public Channel find(long ticketChannelEid){
        return channelRepository.findById(ticketChannelEid)
                .orElseThrow(() -> new EntityNotFoundException("TicketChannel not found for id :" + ticketChannelEid));
    }
    public List<Channel> findTicketChannelByDate(Date start, Date end) {
        return channelRepository.findByRegDateBetweenOrModDateBetween(start, end);
    }
}
