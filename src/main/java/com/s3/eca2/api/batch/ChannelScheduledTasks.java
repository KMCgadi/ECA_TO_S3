package com.s3.eca2.api.batch;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.api.ticketChannel.ChannelToParquetConverter;
import com.s3.eca2.domain.ticketChannel.Channel;
import com.s3.eca2.domain.ticketChannel.ChannelService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

@Component
public class ChannelScheduledTasks {
    private static final Logger logger = LoggerFactory.getLogger(ChannelScheduledTasks.class);
    private final ChannelService channelService;
    private final ChannelToParquetConverter channelToParquetConverter;
    private final S3Service s3Service;

    public ChannelScheduledTasks(ChannelService channelService, ChannelToParquetConverter channelToParquetConverter, S3Service s3Service) {
        this.channelService = channelService;
        this.channelToParquetConverter = channelToParquetConverter;
        this.s3Service = s3Service;
    }

    @Scheduled(cron = "0 0 0 * * *")
    public void performParquetConversion() {
        logger.info("channel batch 시작");
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Seoul"));
        LocalDate yesterday = today.minusDays(1);
        Date start = Date.from(yesterday.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());
        Date end = Date.from(today.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = yesterday.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = yesterday.format(formatterForPath);

        int pageNumber = 0;
        final int pageSize = 400000;
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        try {
            while (true) {
                Page<Channel> channelPage = channelService.findTicketChannelByDate(start, end, pageable);
                List<Channel> channels = channelPage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "eca_cs_ticket_channel_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                channelToParquetConverter.writeTicketChannelToParquet(channels, outputPath);

                String s3Key = "cs/prod/eca_cs_ticket_channel_tm/base_dt=" + formattedDateForPath +
                        "/eca_cs_ticket_channel_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet";
                s3Service.uploadFileToS3(outputPath, s3Key);

                logger.info("Parquet file created and uploaded successfully to: {}", s3Key);

                if (!channelPage.hasNext() || channels.isEmpty()) {
                    break;
                }
                pageNumber++;
                pageable = pageable.next();
            }
        } catch (Exception e) {
            logger.error("Failed to create and upload Parquet file.", e);
        }
    }
}

