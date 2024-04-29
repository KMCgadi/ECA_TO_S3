package com.s3.eca2.api.surveyResult;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.surveyResult.SurveyResult;
import com.s3.eca2.domain.surveyResult.SurveyResultService;
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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

@RestController
@RequestMapping("/rest/api/v1/s3/survey-result")
public class SurveyResultController {
    private static final Logger logger = LoggerFactory.getLogger(SurveyResultController.class);
    private final SurveyResultService surveyResultService;
    private final SurveyResultToParquetConverter surveyResultToParquetConverter;
    private final S3Service s3Service;
    private final SurveyResultToCSVConverter surveyResultToCSVConverter;

    public SurveyResultController(SurveyResultService surveyResultService, SurveyResultToParquetConverter surveyResultToParquetConverter, S3Service s3Service, SurveyResultToCSVConverter surveyResultToCSVConverter) {
        this.surveyResultService = surveyResultService;
        this.surveyResultToParquetConverter = surveyResultToParquetConverter;
        this.s3Service = s3Service;
        this.surveyResultToCSVConverter = surveyResultToCSVConverter;
    }

    @GetMapping("/{surveyEntityId}")
    public SurveyResult selectOne(@PathVariable long surveyEntityId) {
        return surveyResultService.find(surveyEntityId);
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

                Page<SurveyResult> surveyResultPage = surveyResultService.findSurveyResultByDate(startDate, endDate, pageable);
                List<SurveyResult> surveyResults = surveyResultPage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "eca_cs_survey_result_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                surveyResultToParquetConverter.writeSurveyResultToParquet(surveyResults, outputPath);

                String s3Key = "cs/prod/eca_cs_survey_result_tm/base_dt=" + formattedDateForPath +
                        "/eca_cs_survey_result_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet";
                s3Service.uploadFileToS3(outputPath, s3Key);

                logger.info("Parquet file created and uploaded successfully to: {}", s3Key);

                if (!surveyResultPage.hasNext() || surveyResults.isEmpty()) {
                    break;
                }
                pageNumber++;
                pageable = pageable.next();
            }
            return ResponseEntity.ok("Parquet file created and uploaded successfully to: ");
        } catch (Exception e) {
            logger.error(String.valueOf(e));
            return ResponseEntity.internalServerError().body("Failed to create and upload Parquet file.");
        }
    }

    @GetMapping("/makeCSV")
    public ResponseEntity<String> selectByDateCSV(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") Date start,
                                                  @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end) {

        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Asia/Seoul")).minusDays(1);
        DateTimeFormatter fileNameFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        DateTimeFormatter pathFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForFileName = now.format(fileNameFormatter);
        String formattedDateForPath = now.format(pathFormatter);

        int pageNumber = 0;
        final int pageSize = 400000;
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        try {
            while (true) {
                Page<SurveyResult> surveyResultPage = surveyResultService.findSurveyResultByDate(start, end, pageable);
                List<SurveyResult> surveyResults = surveyResultPage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "eca_cs_survey_result_tm" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                surveyResultToCSVConverter.writeSurveyResultToCSV(surveyResults, outputPath);

                String s3Key = "cs/dev/eca_cs_survey_result_tm/base_dt=" + formattedDateForPath +
                        "/eca_cs_survey_result_tm/CSV/eca_cs_survey_result_tm_" + formattedDateForFileName + "_1.csv";
                s3Service.uploadFileToS3(outputPath, s3Key);

                logger.info("Parquet file created and uploaded successfully to: {}", s3Key);

                if (!surveyResultPage.hasNext() || surveyResults.isEmpty()) {
                    break;
                }
                pageNumber++;
                pageable = pageable.next();
            }
            return ResponseEntity.ok("CSV file created and uploaded successfully to");
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
