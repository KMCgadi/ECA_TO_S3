package com.s3.eca2.domain.surveyResult;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;


@Service
public class SurveyResultService {

    private final SurveyResultRepository surveyResultRepository;

    public SurveyResultService(SurveyResultRepository surveyResultRepository){
        this.surveyResultRepository = surveyResultRepository;
    }

    public SurveyResult find(long surveyEntityId) {
      return surveyResultRepository.findById(surveyEntityId)
                .orElseThrow(() ->  new EntityNotFoundException("surveyResult not found for id: " + surveyEntityId));
    }
    public Page<SurveyResult> findSurveyResultByDate(Date start, Date end, Pageable pageable) {
        return surveyResultRepository.findByRegDateBetweenOrModDateBetween(start, end, pageable);
    }
}
