package com.s3.eca2.domain.counselType;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;

public interface CounselTypeRepository extends JpaRepository<CounselType, Long> {
    @Query("SELECT t FROM CounselType t WHERE t.regDate >= :start AND t.regDate <= :end OR t.modDate >= :start AND t.modDate <= :end")
    Page<CounselType> findByRegDateBetweenOrModDateBetween(
            @Param("start") Date start,
            @Param("end") Date end,
            Pageable pageable);
}
