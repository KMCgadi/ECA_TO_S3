package com.s3.eca2.domain.settingsCode;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;

public interface SettingsCodeRepository extends JpaRepository<SettingsCode,Long> {
    @Query("SELECT t FROM SettingsCode t WHERE t.regDate >= :start AND t.regDate <= :end OR t.modDate >= :start AND t.modDate <= :end")
    Page<SettingsCode> findByRegDateBetweenOrModDateBetween(
            @Param("start") Date start,
            @Param("end") Date end,
            Pageable pageable);
}
