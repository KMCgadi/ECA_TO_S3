package com.s3.eca2.api.batch;


import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.api.ticket.TicketToParquetConverter;
import com.s3.eca2.domain.ticket.Ticket;
import com.s3.eca2.domain.ticket.TicketService;
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
        String formattedDateForFileName = yesterday.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = yesterday.format(formatterForPath);

        int pageNumber = 0; // 시작 페이지 번호
        final int pageSize = 400000; // 설정한 페이지 크기
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        try {
            while (true) {
                Page<Ticket> ticketsPage = ticketService.findTicketsByDate(start, end, pageable);
                List<Ticket> tickets = ticketsPage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "eca_cs_ticket_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                ticketToParquetConverter.writeTicketsToParquet(tickets, outputPath);

                String s3Key = "cs/prod/eca_cs_ticket_tm/base_dt=" + formattedDateForPath +
                        "/eca_cs_ticket_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet";
                s3Service.uploadFileToS3(outputPath, s3Key);

                logger.info("Parquet file created and uploaded successfully to: {}", s3Key);

                if (!ticketsPage.hasNext() || tickets.isEmpty()) {
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
