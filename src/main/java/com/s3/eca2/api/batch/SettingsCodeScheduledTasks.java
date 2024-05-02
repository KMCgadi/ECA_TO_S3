package com.s3.eca2.api.batch;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.api.settingsCode.SettingsCodeToParquetConverter;
import com.s3.eca2.domain.settingsCode.SettingsCode;
import com.s3.eca2.domain.settingsCode.SettingsCodeService;
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
public class SettingsCodeScheduledTasks {
    private static final Logger logger = LoggerFactory.getLogger(SettingsCodeScheduledTasks.class);
    private final SettingsCodeService settingsCodeService;
    private final SettingsCodeToParquetConverter settingsCodeToParquetConverter;
    private final S3Service s3Service;

    public SettingsCodeScheduledTasks(SettingsCodeService settingsCodeService, SettingsCodeToParquetConverter settingsCodeToParquetConverter, S3Service s3Service){
        this.settingsCodeService = settingsCodeService;
        this.settingsCodeToParquetConverter = settingsCodeToParquetConverter;
        this.s3Service = s3Service;
    }

    @Scheduled(cron = "0 0 0 * * *")
    public void performParquetConversion() {
        logger.info("SettingsCode batch 시작");
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Seoul"));
        LocalDate yesterday = today.minusDays(1);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = yesterday.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = yesterday.format(formatterForPath);

        int pageNumber = 0;
        final int pageSize = 400000; // 페이지 크기 설정
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        try {
            while (true) {
                Page<SettingsCode> settingsCodePage = settingsCodeService.findAllSettingsCodes(pageable);
                List<SettingsCode> settingsCodes = settingsCodePage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "prf_settings_code_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                settingsCodeToParquetConverter.writeSettingsCodeToParquet(settingsCodes, outputPath);

                String s3Key = "cs/prod/prf_settings_code/base_dt=" + formattedDateForPath +
                        "/prf_settings_code_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet";
                s3Service.uploadFileToS3(outputPath, s3Key);

                logger.info("Parquet file created and uploaded successfully to: {}", s3Key);

                if (!settingsCodePage.hasNext() || settingsCodes.isEmpty()) {
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
