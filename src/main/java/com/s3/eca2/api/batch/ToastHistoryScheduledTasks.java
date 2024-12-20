package com.s3.eca2.api.batch;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.api.toastHistory.ToastHistoryToParquetConverter;
import com.s3.eca2.domain.toastHistory.ToastHistory;
import com.s3.eca2.domain.toastHistory.ToastHistoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;


@Component
public class ToastHistoryScheduledTasks {
    private static final Logger logger = LoggerFactory.getLogger(ToastHistoryScheduledTasks.class);
    private final ToastHistoryService toastHistoryService;
    private final ToastHistoryToParquetConverter toastHistoryToParquetConverter;
    private final S3Service s3Service;

    public ToastHistoryScheduledTasks(ToastHistoryService toastHistoryService, ToastHistoryToParquetConverter toastHistoryToParquetConverter, S3Service s3Service){
        this.toastHistoryService = toastHistoryService;
        this.toastHistoryToParquetConverter = toastHistoryToParquetConverter;
        this.s3Service = s3Service;
    }

    @Scheduled(cron = "0 0 0 * * *")
    public void performParquetConversion() {
        logger.info("ToastHistory batch 시작");
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Seoul"));
        LocalDate yesterday = today.minusDays(1);
        Date start = Date.from(yesterday.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());
        Date end = Date.from(today.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = yesterday.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = yesterday.format(formatterForPath);

        int pageNumber = 0; // 시작 페이지 번호
        final int pageSize = 400000; // 설정한 페이지 크기
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        try {
            while (true) {
                Page<ToastHistory> toastHistoryPage = toastHistoryService.findToastHistoryByDate(start, end, pageable);
                List<ToastHistory> toastHistories = toastHistoryPage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "eca_ts_history_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                toastHistoryToParquetConverter.writeToastHistoryToParquet(toastHistories, outputPath);

                String s3Key = "cs/prod/eca_ts_history_tm/base_dt=" + formattedDateForPath +
                        "/eca_ts_history_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet";
                s3Service.uploadFileToS3(outputPath, s3Key);

                logger.info("Parquet file created and uploaded successfully to: {}", s3Key);

                if (!toastHistoryPage.hasNext() || toastHistories.isEmpty()) {
                    break;
                }
                pageNumber++;
                pageable = pageable.next();
            }
        } catch (Exception e) {
            logger.error("Failed to create and upload Parquet file.", e);
        }
    }
}
