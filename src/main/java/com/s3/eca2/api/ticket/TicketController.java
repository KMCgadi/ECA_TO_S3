package com.s3.eca2.api.ticket;

import com.s3.eca2.domain.ticket.Ticket;
import com.s3.eca2.domain.ticket.TicketService;
import com.s3.eca2.s3uploader.S3Uploader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.List;

@RestController
@RequestMapping("/rest/api/v1/s3/ticket")
public class TicketController {
    @Autowired
    S3Uploader s3Uploader;
    private final TicketToParquetConverter ticketToParquetConverter;
    private final TicketService ticketService;

    public TicketController(TicketService ticketService, TicketToParquetConverter ticketToParquetConverter) {
        this.ticketService = ticketService;
        this.ticketToParquetConverter = ticketToParquetConverter;
    }

    @GetMapping("/{entityId}")
    public Ticket selectOne(@PathVariable long entityId) {
        return ticketService.find(entityId);
    }

    @GetMapping("/makeParquet")
    public String selectByDate(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") Date start,
                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end) {
        List<Ticket> tickets = ticketService.findTicketsByDate(start, end);
        String outputPath = "path/to/output/tickets.parquet"; // Parquet 파일의 저장 경로 지정

        try {
            // Ticket 객체를 Parquet 파일로 변환하고 저장하는 로직
            ticketToParquetConverter.writeTicketsToParquet(tickets, outputPath);

            // S3에 Parquet 파일 업로드
            String s3Key = "your/path/in/s3/tickets.parquet"; // S3 내에서의 파일 경로 및 이름
            s3Uploader.uploadFileToS3(outputPath, s3Key);

            return "Parquet file created and uploaded successfully to: " + s3Key;
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to create and upload Parquet file.";
        }
    }

    @GetMapping("/s3")
    public void hello() {
        s3Uploader.listS3BucketContents();
    }
}
