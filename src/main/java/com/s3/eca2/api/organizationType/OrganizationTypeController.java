package com.s3.eca2.api.organizationType;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.attachUrl.AttachUrl;
import com.s3.eca2.domain.organizationType.OrganizationType;
import com.s3.eca2.domain.organizationType.OrganizationTypeService;

import org.springframework.beans.factory.annotation.Autowired;
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
@RequestMapping("/rest/api/v1/s3/organizationType")
public class OrganizationTypeController {
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

    @GetMapping("/makeParquet")
    public ResponseEntity<String> selectByDate(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") Date start,
                                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end) {

        ZonedDateTime date = ZonedDateTime.now(ZoneId.of("Asia/Seoul"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        String formattedDate = date.format(formatter);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", formattedDate + ".parquet").toString();
        System.out.println("경로체크: " + outputPath);

        try {
            List<OrganizationType> organizationTypes = organizationTypeService.findOrganizationTypeByDate(start, end);
            organizationToParquetConverter.writeOrganizationTypeToParquet(organizationTypes, outputPath);

            String s3Key = "cs/dev/gaea_organization_type_tm/base_dt=" + formattedDate;
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
