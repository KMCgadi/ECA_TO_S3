package com.s3.eca2.domain.ticketChannel;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;

public interface ChannelRepository extends JpaRepository<Channel, Long> {
    @Query("SELECT t FROM Channel t WHERE t.regDate >= :start AND t.regDate <= :end OR t.modDate >= :start AND t.modDate <= :end")
    Page<Channel> findByRegDateBetweenOrModDateBetween(
            @Param("start") Date start,
            @Param("end") Date end,
            Pageable pageable);
}
