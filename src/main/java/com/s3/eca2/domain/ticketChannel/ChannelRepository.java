package com.s3.eca2.domain.ticketChannel;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;


public interface ChannelRepository extends JpaRepository<Channel, Long> {
    @Query("SELECT t FROM Channel t WHERE t.regDate >= :start AND t.regDate <= :end OR t.modDate >= :start AND t.modDate <= :end")
    List<Channel> findByRegDateBetweenOrModDateBetween(@Param("start") Date start, @Param("end") Date end);
}
