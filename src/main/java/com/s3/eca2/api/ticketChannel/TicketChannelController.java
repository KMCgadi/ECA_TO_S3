package com.s3.eca2.api.ticketChannel;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.ticketChannel.TicketChannel;
import com.s3.eca2.domain.ticketChannel.TicketChannelService;
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
@RequestMapping("/rest/api/v1/s3/ticketChannel")
public class TicketChannelController {
    private final S3Service s3Service;
    private final TicketChannelToParquetConverter ticketChannelToParquetConverter;
    private final TicketChannelService ticketChannelService;

    public TicketChannelController(TicketChannelService ticketChannelService, TicketChannelToParquetConverter ticketChannelToParquetConverter, S3Service s3Service) {
        this.ticketChannelService = ticketChannelService;
        this.ticketChannelToParquetConverter = ticketChannelToParquetConverter;
        this.s3Service = s3Service;
    }

    @GetMapping("/{ticketChannelEid}")
    public TicketChannel selectOne(@PathVariable long ticketChannelEid) {
        return ticketChannelService.find(ticketChannelEid);
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
            List<TicketChannel> ticketChannels = ticketChannelService.findTicketChannelByDate(start, end);
            ticketChannelToParquetConverter.writeTicketChannelToParquet(ticketChannels, outputPath);
            String s3Key = "cs/dev/eca_ticket_channel_tm/base_dt=" + formattedDate;
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
