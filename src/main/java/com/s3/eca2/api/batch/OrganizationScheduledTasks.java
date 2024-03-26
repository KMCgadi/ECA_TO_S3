package com.s3.eca2.api.batch;

import com.s3.eca2.api.organizationType.OrganizationToParquetConverter;
import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.organizationType.OrganizationType;
import com.s3.eca2.domain.organizationType.OrganizationTypeService;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;

@Component
public class OrganizationScheduledTasks {
    private static final Logger logger = LoggerFactory.getLogger(OrganizationScheduledTasks.class);
    private final OrganizationTypeService organizationTypeService;
    private final OrganizationToParquetConverter organizationToParquetConverter;
    private final S3Service s3Service;

public OrganizationScheduledTasks(OrganizationTypeService organizationTypeService, OrganizationToParquetConverter organizationToParquetConverter, S3Service s3Service) {
    this.organizationTypeService = organizationTypeService;
    this.organizationToParquetConverter = organizationToParquetConverter;
    this.s3Service = s3Service;
}
    @Scheduled(cron = "0 0 0 * * *")
    public void performOrganizationTypeParquetConversion() {
        logger.info("Organization batch 시작");
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Seoul"));
        LocalDate yesterday = today.minusDays(1);
        Date start = Date.from(yesterday.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());
        Date end = Date.from(today.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = today.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = today.format(formatterForPath);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", "gaea_organization_type_tm_" + formattedDateForFileName + "_1.parquet").toString();

        try {
            List<OrganizationType> organizationTypes = organizationTypeService.findOrganizationTypeByDate(start, end);
            organizationToParquetConverter.writeOrganizationTypeToParquet(organizationTypes, outputPath);

            String s3Key = "cs/prod/gaea_organization_type_tm/base_dt=" + formattedDateForPath + "/gaea_organization_type_tm_" + formattedDateForFileName + "_1.parquet";
            s3Service.uploadFileToS3(outputPath, s3Key);

            logger.info("Parquet file created and uploaded successfully to: {}", s3Key);
        } catch (Exception e) {
            logger.error("Failed to create and upload Parquet file.", e);
        }
    }
}

