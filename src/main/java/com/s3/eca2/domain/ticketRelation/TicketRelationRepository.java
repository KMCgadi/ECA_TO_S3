package com.s3.eca2.domain.ticketRelation;

import com.s3.eca2.domain.attachUrl.AttachUrl;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;

public interface TicketRelationRepository extends JpaRepository<TicketRelation, Long> {
    @Query("SELECT t FROM TicketRelation t WHERE t.regDate >= :start AND t.regDate <= :end OR t.modDate >= :start AND t.modDate <= :end")
    List<TicketRelation> findByRegDateBetweenOrModDateBetween(@Param("start") Date start, @Param("end") Date end);
}
