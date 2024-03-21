package com.s3.eca2.domain.ticketOrder;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;

public interface TicketOrderRepository extends JpaRepository<TicketOrder, Long> {
    @Query("SELECT t FROM TicketOrder t WHERE t.regDate >= :start AND t.regDate <= :end OR t.modDate >= :start AND t.modDate <= :end")
    List<TicketOrder> findByRegDateBetweenOrModDateBetween(@Param("start") Date start, @Param("end") Date end);
}
