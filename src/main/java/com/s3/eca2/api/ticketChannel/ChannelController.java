package com.s3.eca2.api.ticketChannel;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.ticketChannel.Channel;
import com.s3.eca2.domain.ticketChannel.ChannelService;
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
        final int pageSize = 400000;
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        try {
            while (true) {
                Page<Channel> channelPage = channelService.findTicketChannelByDate(start, end, pageable);
                List<Channel> channels = channelPage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "eca_cs_ticket_channel_tm" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                channelToParquetConverter.writeTicketChannelToParquet(channels, outputPath);

                String s3Key = "cs/prod/eca_cs_ticket_channel_tm/base_dt=" + formattedDateForPath +
                        "/eca_cs_ticket_channel_tm" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet";
                s3Service.uploadFileToS3(outputPath, s3Key);

                if (!channelPage.hasNext() || channels.isEmpty()) {
                    break;
                }
                pageNumber++;
                pageable = pageable.next();
            }
            return ResponseEntity.ok("Parquet files created and uploaded successfully");
        } catch (Exception e) {
            logger.error(String.valueOf(e));
            return ResponseEntity.internalServerError().body("Failed to create and upload Parquet files");
        }
    }

    @GetMapping("/getParquet")
    public ResponseEntity<String> selectByPath(@RequestParam("s3key") String s3key) {
        return ResponseEntity.ok(s3Service.readParquetFromS3(s3key));
    }
}
