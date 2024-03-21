package com.s3.eca2.api.surveyResult;

import com.s3.eca2.domain.surveyResult.SurveyResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Component
public class SurveyResultToCSVConverter {

    private static final Logger logger = LoggerFactory.getLogger(SurveyResultToCSVConverter.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public void writeSurveyResultToCSV(List<SurveyResult> surveyResults, String outputPath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            // CSV 헤더 작성
            writer.write("surveyEntityId,ticketEid,templateId,templateTitle,token,question01,answer01,question02,answer02,question03,answer03,question04,answer04,question05,answer05,question06,answer06,question07,answer07,question08,answer08,question09,answer09,question10,answer10,sendDate,responseDate,entityStatus,modDate,regDate,modUserEntityId,regUserEntityId,counselTypeLargeCd,counselTypeMediumCd,counselTypeSmallCd,contactCd,managerEid\n");

            // 각 surveyResult를 한 줄씩 CSV로 변환
            for (SurveyResult result : surveyResults) {
                // 모든 필드에 대해 null 체크하고, null이면 "null" 문자열 사용
                List<String> values = Arrays.asList(
                        String.valueOf(result.getSurveyEntityId()),
                        String.valueOf(result.getTicketEid()),
                        nullToNullString(result.getTemplateId()),
                        nullToNullString(result.getTemplateTitle()),
                        nullToNullString(result.getToken()),
                        nullToNullString(result.getQuestion01()),
                        nullToNullString(result.getQuestion02()),
                        nullToNullString(result.getQuestion03()),
                        nullToNullString(result.getQuestion04()),
                        nullToNullString(result.getQuestion05()),
                        nullToNullString(result.getQuestion06()),
                        nullToNullString(result.getQuestion07()),
                        nullToNullString(result.getQuestion08()),
                        nullToNullString(result.getQuestion09()),
                        nullToNullString(result.getQuestion10()),
                        nullToNullString(result.getAnswer01()),
                        nullToNullString(result.getAnswer02()),
                        nullToNullString(result.getAnswer03()),
                        nullToNullString(result.getAnswer04()),
                        nullToNullString(result.getAnswer05()),
                        nullToNullString(result.getAnswer06()),
                        nullToNullString(result.getAnswer07()),
                        nullToNullString(result.getAnswer08()),
                        nullToNullString(result.getAnswer09()),
                        nullToNullString(result.getAnswer10()),
                        dateFormatOrNull(result.getSendDate()),
                        dateFormatOrNull(result.getResponseDate()),
                        nullToNullString(result.getEntityStatus()),
                        dateFormatOrNull(result.getModDate()),
                        dateFormatOrNull(result.getRegDate()),
                        String.valueOf(result.getModUserEntityId()),
                        String.valueOf(result.getRegUserEntityId()),
                        nullToNullString(result.getCounselTypeLargeCode()),
                        nullToNullString(result.getCounselTypeMediumCode()),
                        nullToNullString(result.getCounselTypeSmallCode()),
                        nullToNullString(result.getContactCode()),
                        String.valueOf(result.getManagerEid())
                );

                String csvLine = String.join(",", values);
                writer.write(csvLine);
                writer.newLine();
            }
        } catch (IOException e) {
            logger.error("CSV 파일 작성 중 오류 발생: ", e);
        }
    }

    private String nullToNullString(String value) {
        return value != null ? value : "null";
    }

    private String dateFormatOrNull(Date date) {
        return date != null ? dateFormat.format(date) : "null";
    }

}
