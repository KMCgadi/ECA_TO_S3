package com.s3.eca2.domain.organizationType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;

public interface OrganizationRepository extends JpaRepository<OrganizationType,Long> {
    @Query("SELECT t FROM OrganizationType t WHERE t.regDate >= :start AND t.regDate <= :end")
    List<OrganizationType> findByRegDateBetweenOrModDateBetween(@Param("start") Date start, @Param("end") Date end);
}
