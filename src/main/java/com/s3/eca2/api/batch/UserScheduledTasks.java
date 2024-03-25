package com.s3.eca2.api.batch;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.api.user.UserToParquetConverter;
import com.s3.eca2.domain.user.User;
import com.s3.eca2.domain.user.UserService;
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
public class UserScheduledTasks {
    private static final Logger logger = LoggerFactory.getLogger(UserScheduledTasks.class);
    private final UserService userService;
    private final UserToParquetConverter userToParquetConverter;
    private final S3Service s3Service;

    public UserScheduledTasks(UserService userService, UserToParquetConverter userToParquetConverter, S3Service s3Service){
        this.userService = userService;
        this.userToParquetConverter = userToParquetConverter;
        this.s3Service = s3Service;
    }
    @Scheduled(cron = "0 0 0 * * *")
    public void performParquetConversion() {
        logger.info("User batch 시작");
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Seoul"));
        LocalDate yesterday = today.minusDays(1);
        Date start = Date.from(yesterday.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());
        Date end = Date.from(today.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = today.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = today.format(formatterForPath);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", "gaea_user_tm_" + formattedDateForFileName + "_1.parquet").toString();

        try {
            List<User> users = userService.findUserByDate(start, end);
            userToParquetConverter.writeUserToParquet(users, outputPath);

            String s3Key = "cs/dev/gaea_user_tm/base_dt=" + formattedDateForPath + "/gaea_user_tm_" + formattedDateForFileName + "_1.parquet";
            s3Service.uploadFileToS3(outputPath, s3Key);

            logger.info("Parquet file created and uploaded successfully to: {}", s3Key);
        } catch (Exception e) {
            logger.error("Failed to create and upload Parquet file.", e);
        }
    }
}
