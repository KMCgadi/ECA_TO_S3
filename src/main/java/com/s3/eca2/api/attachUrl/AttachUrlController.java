package com.s3.eca2.api.attachUrl;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.attachUrl.AttachUrl;
import com.s3.eca2.domain.attachUrl.AttachUrlService;
import com.s3.eca2.domain.ticket.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
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
@RequestMapping("/rest/api/v1/s3/attach-url")
public class AttachUrlController {
    private static final Logger logger = LoggerFactory.getLogger(AttachUrlController.class);
    private final S3Service s3Service;
    private final AttachUrlService attachUrlService;
    private final AttachUrlToParquetConverter attachUrlToParquetConverter;
    private final AttachUrlToCSVConverter attachUrlToCSVConverter;

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
                                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end) {

        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Asia/Seoul")).minusDays(1);
        DateTimeFormatter fileNameFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        DateTimeFormatter pathFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForFileName = now.format(fileNameFormatter);
        String formattedDateForPath = now.format(pathFormatter);

        int pageNumber = 0;
        final int pageSize = 400000; // 한 페이지 당 처리할 데이터 수를 줄입니다.
        Pageable pageable = PageRequest.of(pageNumber, pageSize);
        try {
            while (true) {
                Page<AttachUrl> attachUrlPage = attachUrlService.findAttachUrlByDate(start, end, pageable);
                List<AttachUrl> attachUrls = attachUrlPage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "eca_ct_attach_url_tm" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                attachUrlToParquetConverter.writeAttachUrlToParquet(attachUrls, outputPath);

                String s3Key = "cs/prod/eca_ct_attach_url_tm/base_dt=" + formattedDateForPath +
                        "/eca_ct_attach_url_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet";
                s3Service.uploadFileToS3(outputPath, s3Key);

                logger.info("Parquet file created and uploaded successfully to: {}", s3Key);

                if (!attachUrlPage.hasNext() || attachUrls.isEmpty()) {
                    break;
                }
                pageNumber++;
                pageable = pageable.next();
            }
            return ResponseEntity.ok("Parquet file created and uploaded successfully to");
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

        int pageNumber = 0;
        final int pageSize = 400000; // 한 페이지 당 처리할 데이터 수를 줄입니다.
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        try {
            while (true) {

                Page<AttachUrl> attachUrlPage = attachUrlService.findAttachUrlByDate(start, end, pageable);
                List<AttachUrl> attachUrls = attachUrlPage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "eca_ct_attach_url_tm" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                attachUrlToCSVConverter.writeAttachUrlToCSV(attachUrls, outputPath);

                String s3Key = "cs/prod/eca_ct_attach_url_tm/base_dt=" + formattedDateForPath +
                        "/eca_ct_attach_url_tm/CSV/eca_ct_attach_url_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet";
                s3Service.uploadFileToS3(outputPath, s3Key);

                if (!attachUrlPage.hasNext() || attachUrls.isEmpty()) {
                    break; // 조회된 데이터가 없거나 마지막 페이지에 도달하면 종료
                }
                pageNumber++;
                pageable = pageable.next();
            }
            return ResponseEntity.ok("Parquet files created and uploaded successfully.");
        } catch (Exception e) {
            logger.error(String.valueOf(e));
            return ResponseEntity.internalServerError().body("Failed to create and upload Parquet files.");
        }
    }

    @GetMapping("/getParquet")
    public ResponseEntity<String> selectByPath(@RequestParam("s3key") String s3key) {
        return ResponseEntity.ok(s3Service.readParquetFromS3(s3key));
    }
}
