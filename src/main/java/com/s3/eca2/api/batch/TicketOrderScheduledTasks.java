package com.s3.eca2.api.batch;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.api.ticketOrder.TicketOrderToParquetConverter;
import com.s3.eca2.domain.ticketOrder.TicketOrder;
import com.s3.eca2.domain.ticketOrder.TicketOrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

@Component
public class TicketOrderScheduledTasks {
    private static final Logger logger = LoggerFactory.getLogger(TicketScheduledTasks.class);
    private final TicketOrderService ticketOrderService;
    private final TicketOrderToParquetConverter ticketOrderToParquetConverter;
    private final S3Service s3Service;

    public TicketOrderScheduledTasks(TicketOrderService ticketOrderService, TicketOrderToParquetConverter ticketOrderToParquetConverter, S3Service s3Service) {
        this.ticketOrderService = ticketOrderService;
        this.ticketOrderToParquetConverter = ticketOrderToParquetConverter;
        this.s3Service = s3Service;
    }

    @Scheduled(cron = "0 0 0 * * *")
    public void performParquetConversion() {
        logger.info("TicketOrder batch 시작");
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Seoul"));
        LocalDate yesterday = today.minusDays(1);
        Date start = Date.from(yesterday.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());
        Date end = Date.from(today.atStartOfDay(ZoneId.of("Asia/Seoul")).toInstant());

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = today.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = today.format(formatterForPath);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", "eca_cs_ticket_order_tm_" + formattedDateForFileName + "_1.parquet").toString();

        try {
            List<TicketOrder> ticketOrders = ticketOrderService.findTicketOrderByDate(start, end);
            ticketOrderToParquetConverter.writeTicketOrderToParquet(ticketOrders, outputPath);

            String s3Key = "cs/prod/eca_cs_ticket_order_tm/base_dt=" + formattedDateForPath + "/eca_cs_ticket_order_tm_" + formattedDateForFileName + "_1.parquet";
            s3Service.uploadFileToS3(outputPath, s3Key);

            logger.info("Parquet file created and uploaded successfully to: {}", s3Key);
        } catch (Exception e) {
            logger.error("Failed to create and upload Parquet file.", e);
        }
    }
}

