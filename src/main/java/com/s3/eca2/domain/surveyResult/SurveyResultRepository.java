package com.s3.eca2.domain.surveyResult;

import com.s3.eca2.domain.attachUrl.AttachUrl;
import com.s3.eca2.domain.organizationType.OrganizationType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;

public interface SurveyResultRepository extends JpaRepository<SurveyResult, Long> {
    @Query("SELECT t FROM SurveyResult t WHERE t.regDate >= :start AND t.regDate <= :end OR t.modDate >= :start AND t.modDate <= :end")
    List<SurveyResult> findByRegDateBetweenOrModDateBetween(@Param("start") Date start, @Param("end") Date end);
}
