package com.s3.eca2.domain.ticketChannel;

import com.s3.eca2.domain.attachUrl.AttachUrl;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;


public interface TicketChannelRepository extends JpaRepository<TicketChannel, Long> {
    @Query("SELECT t FROM TicketChannel t WHERE t.regDate >= :start AND t.regDate <= :end OR t.modDate >= :start AND t.modDate <= :end")
    List<TicketChannel> findByRegDateBetweenOrModDateBetween(@Param("start") Date start, @Param("end") Date end);
}
