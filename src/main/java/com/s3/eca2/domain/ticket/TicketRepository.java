package com.s3.eca2.domain.ticket;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;

public interface TicketRepository extends JpaRepository<Ticket, Long> {
    @Query("SELECT t FROM Ticket t WHERE t.regDate >= :start AND t.regDate <= :end")
    List<Ticket> findByRegDateBetweenOrModDateBetween(@Param("start") Date start, @Param("end") Date end);
}
