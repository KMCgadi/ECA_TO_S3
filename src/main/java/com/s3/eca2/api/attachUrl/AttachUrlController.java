package com.s3.eca2.api.attachUrl;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.attachUrl.AttachUrl;
import com.s3.eca2.domain.attachUrl.AttachUrlService;
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
@RequestMapping("/rest/api/v1/s3/attachUrl")
public class AttachUrlController {
    private final S3Service s3Service;
    private final AttachUrlService attachUrlService;
    private final AttachUrlToParquetConverter attachUrlToParquetConverter;

    public AttachUrlController(AttachUrlService attachUrlService, AttachUrlToParquetConverter attachUrlToParquetConverter, S3Service s3Service) {
        this.attachUrlService = attachUrlService;
        this.attachUrlToParquetConverter = attachUrlToParquetConverter;
        this.s3Service = s3Service;
    }

    @GetMapping("/{attachUrlEid}")
    public AttachUrl selectOne(@PathVariable long attachUrlEid) {
        return attachUrlService.find(attachUrlEid);
    }

    @GetMapping("/makeParquet")
    public ResponseEntity<String> selectByDate(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") Date start,
                                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end) {

        ZonedDateTime date = ZonedDateTime.now(ZoneId.of("Asia/Seoul"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        String formattedDate = date.format(formatter);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", formattedDate + ".parquet").toString();
        System.out.println("경로체크: " + outputPath);

        try {
            List<AttachUrl> attachUrls = attachUrlService.findAttachUrlByDate(start, end);
            attachUrlToParquetConverter.writeAttachUrlToParquet(attachUrls, outputPath);

            String s3Key = "cs/dev/eca_ct_attachUrl_tm/base_dt=" + formattedDate;
            s3Service.uploadFileToS3(outputPath, s3Key);

            return ResponseEntity.ok("Parquet file created and uploaded successfully to: " + s3Key);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Failed to create and upload Parquet file.");
        }
    }

    @GetMapping("/getParquet")
    public ResponseEntity<String> selectByPath(@RequestParam("s3key") String s3key) {
        return ResponseEntity.ok(s3Service.readParquetFromS3(s3key));
    }
}
