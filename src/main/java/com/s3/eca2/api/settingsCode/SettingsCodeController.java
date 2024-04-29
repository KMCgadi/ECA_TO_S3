package com.s3.eca2.api.settingsCode;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.settingsCode.SettingsCode;
import com.s3.eca2.domain.settingsCode.SettingsCodeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;


@RestController
@RequestMapping("/rest/api/v1/s3/settings-code")
public class SettingsCodeController {
    private static final Logger logger = LoggerFactory.getLogger(SettingsCodeController.class);
    private final SettingsCodeService settingsCodeService;
    private final SettingsCodeToParquetConverter settingsCodeToParquetConverter;
    private final S3Service s3Service;

    public SettingsCodeController(SettingsCodeService settingsCodeService, SettingsCodeToParquetConverter settingsCodeToParquetConverter, S3Service s3Service) {
        this.settingsCodeService = settingsCodeService;
        this.settingsCodeToParquetConverter = settingsCodeToParquetConverter;
        this.s3Service = s3Service;
    }

    @GetMapping("/{entityId}")
    public SettingsCode selectOne(@PathVariable long entityId) {
        return settingsCodeService.find(entityId);
    }

    @PostMapping("/makeParquet")
    public ResponseEntity<String> selectByDate(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate start,
                                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate end) {

        DateTimeFormatter fileNameFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        DateTimeFormatter pathFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForFileName = end.format(fileNameFormatter);
        String formattedDateForPath = end.format(pathFormatter);

        int pageNumber = 0;
        final int pageSize = 400000;
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        try {
            while (true) {
                Date startDate = Date.from(start.atStartOfDay(ZoneId.systemDefault()).toInstant());
                Date endDate = Date.from(end.atStartOfDay(ZoneId.systemDefault()).toInstant());

                Page<SettingsCode> settingsCodePage = settingsCodeService.findSettingsCodeByDate(startDate, endDate, pageable);
                List<SettingsCode> settingsCodes = settingsCodePage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "prf_settings_code_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                settingsCodeToParquetConverter.writeSettingsCodeToParquet(settingsCodes, outputPath);

                String s3Key = "cs/prod/prf_settings_code/base_dt=" + formattedDateForPath +
                        "/prf_settings_code_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet";
                s3Service.uploadFileToS3(outputPath, s3Key);

                if (!settingsCodePage.hasNext() || settingsCodes.isEmpty()) {
                    break;
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
