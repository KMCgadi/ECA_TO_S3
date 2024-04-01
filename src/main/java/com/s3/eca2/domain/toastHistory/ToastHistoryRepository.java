package com.s3.eca2.domain.toastHistory;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;

public interface ToastHistoryRepository extends JpaRepository<ToastHistory, Long> {
    @Query("SELECT t FROM ToastHistory t WHERE t.regDate >= :start AND t.regDate <= :end OR t.modDate >= :start AND t.modDate <= :end")
    Page<ToastHistory> findByRegDateBetweenOrModDateBetween(@Param("start") Date start,
                                                            @Param("end") Date end,
                                                            Pageable pageable);
}
