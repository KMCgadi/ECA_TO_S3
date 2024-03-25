package com.s3.eca2.api.attachUrl;

import com.s3.eca2.domain.attachUrl.AttachUrl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Component
public class AttachUrlToCSVConverter {

    private static final Logger logger = LoggerFactory.getLogger(AttachUrlToCSVConverter.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeAttachUrlToCSV(List<AttachUrl> attachUrls, String outputPath) {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath), StandardCharsets.UTF_8))) {
            writer.write('\ufeff');

            writer.write("ATTACH_URL_EID,LINK_TYPE,TICKET_ID,SHORT_URL,SEND_DATE,SUBMIT_DATE,LINK_STATUS,ENTITY_STATUS,REG_DATE,REG_USER_ENTITY_ID,MOD_DATE,MOD_USER_ENTITY_ID\n");

            for (AttachUrl attachUrl : attachUrls) {
                writer.write(toCsvString(attachUrl));
                writer.newLine();
            }
        } catch (IOException e) {
            logger.error("CSV 파일 작성 중 오류 발생: ", e);
        }
    }

    private String toCsvString(AttachUrl attachUrl) {
        return String.format(
                "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
                LongEscapeCsv(attachUrl.getAttachUrlEid()),
                escapeCsv(attachUrl.getLinkType()),
                LongEscapeCsv(attachUrl.getTicketId()),
                escapeCsv(attachUrl.getShortUrl()),
                dateToString(attachUrl.getSendDate()),
                dateToString(attachUrl.getSubmitDate()),
                escapeCsv(attachUrl.getLinkStatus()),
                escapeCsv(attachUrl.getEntityStatus()),
                dateToString(attachUrl.getRegDate()),
                escapeCsv(attachUrl.getRegUserEntityId()),
                dateToString(attachUrl.getModDate()),
                escapeCsv(attachUrl.getModUserEntityId())
        );
    }

    private String escapeCsv(String value) {
        if (value == null) return "";
        value = value.replace("*", "");
        value = value.replace("\n", " ");
        return value.replace("\"", "\"\"");
    }

    private String dateToString(Date date) {
        return date != null ? dateFormat.format(date) : "";
    }

    private String LongEscapeCsv(Long value) {
        return value != null ? String.valueOf(value) : "";
    }
}
