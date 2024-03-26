package com.s3.eca2.api.batch;


import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.api.ticket.TicketToParquetConverter;
import com.s3.eca2.domain.ticket.Ticket;
import com.s3.eca2.domain.ticket.TicketService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

@Component
public class TicketScheduledTasks {
    private static final Logger logger = LoggerFactory.getLogger(TicketScheduledTasks.class);
    private final TicketService ticketService;
    private final TicketToParquetConverter ticketToParquetConverter;
    private final S3Service s3Service;

    public TicketScheduledTasks(TicketService ticketService, TicketToParquetConverter ticketToParquetConverter, S3Service s3Service){
        this.ticketService = ticketService;
        this.ticketToParquetConverter = ticketToParquetConverter;
        this.s3Service = s3Service;
    }

    @Scheduled(cron = "0 0 0 * * *")
    public void performParquetConversion() {
        logger.info("Ticket batch 시작");
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Seoul"));
        LocalDate yesterday = today.minusDays(1);
        Date start = Date.from(yesterday.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());
        Date end = Date.from(today.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = today.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = today.format(formatterForPath);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", "eca_cs_ticket_tm_" + formattedDateForFileName + "_1.parquet").toString();

        try {
            List<Ticket> attachUrls = ticketService.findTicketsByDate(start, end);
            ticketToParquetConverter.writeTicketsToParquet(attachUrls, outputPath);

            String s3Key = "cs/prod/eca_cs_ticket_tm/base_dt=" + formattedDateForPath + "/eca_cs_ticket_tm_" + formattedDateForFileName + "_1.parquet";
            s3Service.uploadFileToS3(outputPath, s3Key);

            logger.info("Parquet file created and uploaded successfully to: {}", s3Key);
        } catch (Exception e) {
            logger.error("Failed to create and upload Parquet file.", e);
        }
    }
}
