package com.s3.eca2.api.attachUrl;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.attachUrl.AttachUrl;
import com.s3.eca2.domain.attachUrl.AttachUrlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.nio.file.Paths;


@RestController
@RequestMapping("/rest/api/v1/s3/attach-url")
public class AttachUrlController {
    private final S3Service s3Service;
    private final AttachUrlService attachUrlService;
    private final AttachUrlToParquetConverter attachUrlToParquetConverter;
    private final AttachUrlToCSVConverter attachUrlToCSVConverter;
    private static final Logger logger = LoggerFactory.getLogger(AttachUrlController.class);

    public AttachUrlController(AttachUrlService attachUrlService, AttachUrlToParquetConverter attachUrlToParquetConverter, S3Service s3Service, AttachUrlToCSVConverter attachUrlToCSVConverter) {
        this.attachUrlService = attachUrlService;
        this.attachUrlToParquetConverter = attachUrlToParquetConverter;
        this.s3Service = s3Service;
        this.attachUrlToCSVConverter = attachUrlToCSVConverter;
    }

    @GetMapping("/{attachUrlEid}")
    public AttachUrl selectOne(@PathVariable long attachUrlEid) {
        return attachUrlService.find(attachUrlEid);
    }

    @PostMapping("/makeParquet")
    public ResponseEntity<String> selectByDate(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") Date start,
                                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end, @RequestParam int fileNum) {

        ZonedDateTime date = ZonedDateTime.now(ZoneId.of("Asia/Seoul"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = date.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = date.format(formatterForPath);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", "eca_ct_attach_url_tm" + formattedDateForFileName + "_" + fileNum + ".parquet").toString();

        try {
            List<AttachUrl> attachUrls = attachUrlService.findAttachUrlByDate(start, end);
            attachUrlToParquetConverter.writeAttachUrlToParquet(attachUrls, outputPath);

            String s3Key = "cs/dev/eca_ct_attach_url_tm/base_dt=" + formattedDateForPath + "/eca_ct_attach_url_tm_" + formattedDateForFileName + "_" + fileNum + ".parquet";
            s3Service.uploadFileToS3(outputPath, s3Key);

            return ResponseEntity.ok("Parquet file created and uploaded successfully to: " + s3Key);
        } catch (Exception e) {
            logger.error(String.valueOf(e));
            return ResponseEntity.internalServerError().body("Failed to create and upload Parquet file.");
        }
    }

    @GetMapping("/makeCSV")
    public ResponseEntity<String> selectByDateCSV(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") Date start,
                                                  @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end) {

        ZonedDateTime date = ZonedDateTime.now(ZoneId.of("Asia/Seoul"));
        DateTimeFormatter formatterForFileName = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = date.format(formatterForFileName);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = date.format(formatterForPath);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", "eca_ct_attach_url_tm_" + formattedDateForFileName + "_1.csv").toString();

        try {
            List<AttachUrl> attachUrls = attachUrlService.findAttachUrlByDate(start, end);
            attachUrlToCSVConverter.writeAttachUrlToCSV(attachUrls, outputPath);

            String s3Key = "cs/dev/eca_ct_attach_url_tm/base_dt=" + formattedDateForPath + "/eca_ct_attach_url_tm/CSV/eca_ct_attach_url_tm_" + formattedDateForFileName + "_1.csv";
            s3Service.uploadFileToS3(outputPath, s3Key);

            return ResponseEntity.ok("CSV file created and uploaded successfully to: " + s3Key);
        } catch (Exception e) {
            logger.error(String.valueOf(e));
            return ResponseEntity.internalServerError().body("Failed to create and upload CSV file.");
        }
    }

    @GetMapping("/getParquet")
    public ResponseEntity<String> selectByPath(@RequestParam("s3key") String s3key) {
        return ResponseEntity.ok(s3Service.readParquetFromS3(s3key));
    }
}
