package com.s3.eca2.api.organizationType;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.organizationType.OrganizationType;
import com.s3.eca2.domain.organizationType.OrganizationTypeService;
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
@RequestMapping("/rest/api/v1/s3/organization-type")
public class OrganizationTypeController {
    private static final Logger logger = LoggerFactory.getLogger(OrganizationTypeController.class);
    private final OrganizationTypeService organizationTypeService;
    private final OrganizationToParquetConverter organizationToParquetConverter;
    private final S3Service s3Service;

    public OrganizationTypeController(OrganizationTypeService organizationTypeService, OrganizationToParquetConverter organizationToParquetConverter, S3Service s3Service) {
        this.organizationTypeService = organizationTypeService;
        this.organizationToParquetConverter = organizationToParquetConverter;
        this.s3Service = s3Service;
    }

    @GetMapping("/{organizationTypeEid}")
    public OrganizationType selectOne(@PathVariable long organizationTypeEid) {
        return organizationTypeService.find(organizationTypeEid);
    }

    @PostMapping("/makeParquet")
    public ResponseEntity<String> selectByDate(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") Date start,
                                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end) {
        // 날짜와 파일 번호를 포맷팅하기 위한 준비
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

                Page<OrganizationType> organizationTypePage = organizationTypeService.findOrganizationTypeByDate(start, end, pageable);
                List<OrganizationType> organizationTypes = organizationTypePage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "gaea_organization_type_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                organizationToParquetConverter.writeOrganizationTypeToParquet(organizationTypes, outputPath);

                String s3Key = "cs/dev/gaea_organization_type_tm/base_dt=" + formattedDateForPath +
                        "/gaea_organization_type_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet";
                s3Service.uploadFileToS3(outputPath, s3Key);

                if (!organizationTypePage.hasNext() || organizationTypes.isEmpty()) {
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
