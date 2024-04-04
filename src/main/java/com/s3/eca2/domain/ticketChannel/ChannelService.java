package com.s3.eca2.domain.ticketChannel;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;

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
    public Page<Channel> findTicketChannelByDate(Date start, Date end, Pageable pageable) {
        return channelRepository.findByRegDateBetweenOrModDateBetween(start, end, pageable);
    }
}
