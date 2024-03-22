package com.s3.eca2.api.batch;

import com.s3.eca2.api.attachUrl.AttachUrlToParquetConverter;
import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.attachUrl.AttachUrl;
import com.s3.eca2.domain.attachUrl.AttachUrlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

@Component
public class AttachUrlScheduledTasks {

    private static final Logger logger = LoggerFactory.getLogger(AttachUrlScheduledTasks.class);

    private final AttachUrlService attachUrlService;
    private final AttachUrlToParquetConverter attachUrlToParquetConverter;
    private final S3Service s3Service;

    public AttachUrlScheduledTasks(AttachUrlService attachUrlService, AttachUrlToParquetConverter attachUrlToParquetConverter, S3Service s3Service) {
        this.attachUrlService = attachUrlService;
        this.attachUrlToParquetConverter = attachUrlToParquetConverter;
        this.s3Service = s3Service;
    }

    @Scheduled(cron = "0 0 0 * * *") // 매일 자정에 실행
    public void performParquetConversion() {
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Seoul"));
        LocalDate yesterday = today.minusDays(1);
        Date start = Date.from(yesterday.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());
        Date end = Date.from(today.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = today.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = today.format(formatterForPath);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", "eca_ct_attach_url_tm_" + formattedDateForFileName + "_1.parquet").toString();

        try {
            List<AttachUrl> attachUrls = attachUrlService.findAttachUrlByDate(start, end);
            attachUrlToParquetConverter.writeAttachUrlToParquet(attachUrls, outputPath);

            String s3Key = "cs/dev/eca_ct_attach_url_tm/base_dt=" + formattedDateForPath + "/eca_ct_attach_url_tm_" + formattedDateForFileName + "_1.parquet";
            s3Service.uploadFileToS3(outputPath, s3Key);

            logger.info("Parquet file created and uploaded successfully to: {}", s3Key);
        } catch (Exception e) {
            logger.error("Failed to create and upload Parquet file.", e);
        }
    }
}
