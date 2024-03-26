package com.s3.eca2.api.user;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.user.User;
import com.s3.eca2.domain.user.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

@RestController
@RequestMapping("/rest/api/v1/s3/user")
public class UserController {
    private final S3Service s3Service;
    private final UserService userService;
    private final UserToParquetConverter userToParquetConverter;
    private static final Logger logger = LoggerFactory.getLogger(UserController.class);

    public UserController(UserService userService, UserToParquetConverter userToParquetConverter, S3Service s3Service) {
        this.userService = userService;
        this.s3Service = s3Service;
        this.userToParquetConverter = userToParquetConverter;
    }

    @GetMapping("/{userEid}")
    public User selectOne(@PathVariable long userEid) {
        return userService.find(userEid);
    }

    @GetMapping("/makeParquet")
    public ResponseEntity<String> selectByDate(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") Date start,
                                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end, @RequestParam int fileNum) {

        ZonedDateTime date = ZonedDateTime.now(ZoneId.of("Asia/Seoul"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = date.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = date.format(formatterForPath);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", "gaea_user_tm_" + formattedDateForFileName + "_" + fileNum + ".parquet").toString();

        try {
            List<User> users = userService.findUserByDate(start, end);
            userToParquetConverter.writeUserToParquet(users, outputPath);

            String s3Key = "cs/dev/gaea_user_tm/base_dt=" + formattedDateForPath + "/gaea_user_tm_" + formattedDateForFileName + "_" + fileNum + ".parquet";
            s3Service.uploadFileToS3(outputPath, s3Key);

            return ResponseEntity.ok("Parquet file created and uploaded successfully to: " + s3Key);
        } catch (Exception e) {
            logger.error(String.valueOf(e));
            return ResponseEntity.internalServerError().body("Failed to create and upload Parquet file.");
        }
    }

    @GetMapping("/getParquet")
    public ResponseEntity<String> selectByPath(@RequestParam("s3key") String s3key) {
        return ResponseEntity.ok(s3Service.readParquetFromS3(s3key));
    }
}