package com.s3.eca2.domain.surveyResult;

import com.s3.eca2.domain.organizationType.OrganizationType;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;
import java.util.List;


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
    public List<SurveyResult> findSurveyResultByDate(Date start, Date end) {
        return surveyResultRepository.findByRegDateBetweenOrModDateBetween(start, end);
    }
}
