package com.s3.eca2.api.ticketChannel;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.ticketChannel.Channel;
import com.s3.eca2.domain.ticketChannel.ChannelService;
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
@RequestMapping("/rest/api/v1/s3/channel")
public class ChannelController {
    private final S3Service s3Service;
    private final ChannelToParquetConverter channelToParquetConverter;
    private final ChannelService channelService;
    private static final Logger logger = LoggerFactory.getLogger(ChannelController.class);

    public ChannelController(ChannelService channelService, ChannelToParquetConverter channelToParquetConverter, S3Service s3Service) {
        this.channelService = channelService;
        this.channelToParquetConverter = channelToParquetConverter;
        this.s3Service = s3Service;
    }

    @GetMapping("/{ticketChannelEid}")
    public Channel selectOne(@PathVariable long ticketChannelEid) {
        return channelService.find(ticketChannelEid);
    }

    @GetMapping("/makeParquet")
    public ResponseEntity<String> selectByDate(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") Date start,
                                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end, @RequestParam int fileNum) {

        ZonedDateTime date = ZonedDateTime.now(ZoneId.of("Asia/Seoul"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = date.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = date.format(formatterForPath);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", "eca_cs_ticket_channel_tm" + formattedDateForFileName + "_" + fileNum + ".parquet").toString();

        try {
            List<Channel> channels = channelService.findTicketChannelByDate(start, end);
            channelToParquetConverter.writeTicketChannelToParquet(channels, outputPath);

            String s3Key = "cs/dev/eca_cs_ticket_channel_tm/base_dt=" + formattedDateForPath + "/eca_cs_ticket_channel_tm_" + formattedDateForFileName + "_" + fileNum + ".parquet";
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
