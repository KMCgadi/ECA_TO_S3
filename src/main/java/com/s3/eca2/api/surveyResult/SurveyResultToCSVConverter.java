package com.s3.eca2.api.surveyResult;

import com.s3.eca2.domain.surveyResult.SurveyResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Component
public class SurveyResultToCSVConverter {

    private static final Logger logger = LoggerFactory.getLogger(SurveyResultToCSVConverter.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeSurveyResultToCSV(List<SurveyResult> surveyResults, String outputPath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            writer.write(
                    "SURVEY_ENTITY_ID,TICKET_EID,TEMPLATE_ID,TEMPLATE_TITLE,TOKEN,QUESTION_01,QUESTION_02,QUESTION_03,QUESTION_04,QUESTION_05," +
                            "QUESTION_06,QUESTION_07,QUESTION_08,QUESTION_09,QUESTION_10,ANSWER_01,ANSWER_02,ANSWER_03,ANSWER_04,ANSWER_05," +
                            "ANSWER_06,ANSWER_07,ANSWER_08,ANSWER_09,ANSWER_10,SEND_DATE,RESPONSE_DATE,ENTITY_STATUS,MOD_DATE,REG_DATE," +
                            "MOD_USER_ENTITY_ID,REG_USER_ENTITY_ID,COUNSEL_TYPE_LARGE_CD,COUNSEL_TYPE_MEDIUM_CD,COUNSEL_TYPE_SMALL_CD,CONTACT_CD,MANAGER_EID\n"
            );

            for (SurveyResult result : surveyResults) {
                String csvLine = String.format(
                        "%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%s,%s,%s,%s,%d",
                        result.getSurveyEntityId(),
                        result.getTicketEid(),
                        nullToString(result.getTemplateId()),
                        nullToString(result.getTemplateTitle()),
                        nullToString(result.getToken()),
                        nullToString(result.getQuestion01()),
                        nullToString(result.getQuestion02()),
                        nullToString(result.getQuestion03()),
                        nullToString(result.getQuestion04()),
                        nullToString(result.getQuestion05()),
                        nullToString(result.getQuestion06()),
                        nullToString(result.getQuestion07()),
                        nullToString(result.getQuestion08()),
                        nullToString(result.getQuestion09()),
                        nullToString(result.getQuestion10()),
                        nullToString(result.getAnswer01()),
                        nullToString(result.getAnswer02()),
                        nullToString(result.getAnswer03()),
                        nullToString(result.getAnswer04()),
                        nullToString(result.getAnswer05()),
                        nullToString(result.getAnswer06()),
                        nullToString(result.getAnswer07()),
                        nullToString(result.getAnswer08()),
                        nullToString(result.getAnswer09()),
                        nullToString(result.getAnswer10()),
                        dateToString(result.getSendDate()),
                        dateToString(result.getResponseDate()),
                        nullToString(result.getEntityStatus()),
                        dateToString(result.getModDate()),
                        dateToString(result.getRegDate()),
                        result.getModUserEntityId(),
                        result.getRegUserEntityId(),
                        nullToString(result.getCounselTypeLargeCode()),
                        nullToString(result.getCounselTypeMediumCode()),
                        nullToString(result.getCounselTypeSmallCode()),
                        nullToString(result.getContactCode()),
                        result.getManagerEid()
                ).replace("null", ""); // null 값은 빈 문자열로 처리
                writer.write(csvLine);
                writer.newLine();
            }
        } catch (IOException e) {
            logger.error("CSV 파일 작성 중 오류 발생: ", e);
        }
    }

    private String nullToString(String value) {
        return value != null ? value : "";
    }

    private String dateToString(Date date) {
        return date != null ? dateFormat.format(date) : "";
    }
}