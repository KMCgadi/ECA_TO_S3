package com.s3.eca2.api.surveyResult;

import com.s3.eca2.domain.surveyResult.SurveyResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Component
public class SurveyResultToCSVConverter {

    private static final Logger logger = LoggerFactory.getLogger(SurveyResultToCSVConverter.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public void writeSurveyResultToCSV(List<SurveyResult> surveyResults, String outputPath) {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath), StandardCharsets.UTF_8))) {
            writer.write('\ufeff');

            writer.write(
                    "SURVEY_ENTITY_ID,TICKET_EID,TEMPLATE_ID,TEMPLATE_TITLE,TOKEN,QUESTION_01,QUESTION_02,QUESTION_03,QUESTION_04,QUESTION_05," +
                            "QUESTION_06,QUESTION_07,QUESTION_08,QUESTION_09,QUESTION_10,ANSWER_01,ANSWER_02,ANSWER_03,ANSWER_04,ANSWER_05," +
                            "ANSWER_06,ANSWER_07,ANSWER_08,ANSWER_09,ANSWER_10,SEND_DATE,RESPONSE_DATE,ENTITY_STATUS,MOD_DATE,REG_DATE," +
                            "MOD_USER_ENTITY_ID,REG_USER_ENTITY_ID,COUNSEL_TYPE_LARGE_CD,COUNSEL_TYPE_MEDIUM_CD,COUNSEL_TYPE_SMALL_CD,CONTACT_CD,MANAGER_EID\n"
            );

            for (SurveyResult result : surveyResults) {
                writer.write(toCsvString(result));
                writer.newLine();
            }
        } catch (IOException e) {
            logger.error("CSV 파일 작성 중 오류 발생: ", e);
        }
    }

    private String toCsvString(SurveyResult result) {
        return String.format(
                "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
                LongEscapeCsv(result.getSurveyEntityId()),
                LongEscapeCsv(result.getTicketEid()),
                escapeCsv(result.getTemplateId()),
                escapeCsv(result.getTemplateTitle()),
                escapeCsv(result.getToken()),
                escapeCsv(result.getQuestion01()),
                escapeCsv(result.getQuestion02()),
                escapeCsv(result.getQuestion03()),
                escapeCsv(result.getQuestion04()),
                escapeCsv(result.getQuestion05()),
                escapeCsv(result.getQuestion06()),
                escapeCsv(result.getQuestion07()),
                escapeCsv(result.getQuestion08()),
                escapeCsv(result.getQuestion09()),
                escapeCsv(result.getQuestion10()),
                escapeCsv(result.getAnswer01()),
                escapeCsv(result.getAnswer02()),
                escapeCsv(result.getAnswer03()),
                escapeCsv(result.getAnswer04()),
                escapeCsv(result.getAnswer05()),
                escapeCsv(result.getAnswer06()),
                escapeCsv(result.getAnswer07()),
                escapeCsv(result.getAnswer08()),
                escapeCsv(result.getAnswer09()),
                escapeCsv(result.getAnswer10()),
                dateToString(result.getSendDate()),
                dateToString(result.getResponseDate()),
                escapeCsv(result.getEntityStatus()),
                dateToString(result.getModDate()),
                dateToString(result.getRegDate()),
                LongEscapeCsv(result.getModUserEntityId()),
                LongEscapeCsv(result.getRegUserEntityId()),
                escapeCsv(result.getCounselTypeLargeCode()),
                escapeCsv(result.getCounselTypeMediumCode()),
                escapeCsv(result.getCounselTypeSmallCode()),
                escapeCsv(result.getContactCode()),
                result.getManagerEid()
        );
    }

    private String escapeCsv(String value) {
        if (value == null) return "";
        value = value.replace("*", "");
        value = value.replace("\n", " ");
        return value;
    }

    private String dateToString(Date date) {
        return date != null ? dateFormat.format(date) : "";
    }
    private String LongEscapeCsv(Long value) {
        return value != null ? String.valueOf(value) : "";
    }

}