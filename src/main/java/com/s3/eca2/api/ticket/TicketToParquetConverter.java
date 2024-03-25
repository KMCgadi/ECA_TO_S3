package com.s3.eca2.api.ticket;

import com.s3.eca2.domain.ticket.Ticket;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

@Component
public class TicketToParquetConverter {

    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(INT64, REQUIRED).named("ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("TITLE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("TYPE_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("STATUS_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("PRIORITY_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("COUNSEL_CLAS_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("COUNSEL_TYPE_LARGE_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("COUNSEL_TYPE_MEDIUM_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("COUNSEL_TYPE_SMALL_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("MEMO"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("CUSTOMER_TYPE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("RESERVATION_TIME"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("MANAGER_EID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ALIMTALK_SEND_YN"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ENTITY_STATUS"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("REG_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("REG_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("MOD_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("MOD_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("NPS_UPDATE_YN"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("SUGGESTION_YN"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("COMPLETE_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("TEMPLATE_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("START_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("END_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("SEND_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("SURVEY_STATUS_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("VOC_TRNS_YN"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("COUNSEL_CATEGORY_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("TRANSFER_YN"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("INIT_QUEUE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("TOBE_QUEUE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("RESERVE_STAT"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("STAT_HISTORY"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("RESERVATION_TIME2"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("MODIFY_NUMBER"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("RESERVATION_PERMIT"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("RESERVE_MOD_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("TRANSFER_TEMPLATE_CONTENT"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("MANAGER_MOD_DATE"))
            .named("Ticket");
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger logger = LoggerFactory.getLogger(TicketToParquetConverter.class);

    public void writeTicketsToParquet(List<Ticket> tickets, String fileOutputPath) throws IOException {
        try (ParquetWriter<Ticket> writer = new TicketParquetWriter(new Path(fileOutputPath), SCHEMA)) {
            for (Ticket ticket : tickets) {
                writer.write(ticket);
            }
        } catch (Exception e) {
            logger.error(String.valueOf(e));
        }
    }

    private static class TicketParquetWriter extends ParquetWriter<Ticket> {
        public TicketParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new TicketWriteSupport(schema));
        }
    }

    private static class TicketWriteSupport extends WriteSupport<Ticket> {
        private MessageType schema;
        private RecordConsumer recordConsumer;

        public TicketWriteSupport(MessageType schema) {
            this.schema = schema;
        }

        @Override
        public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
            return new WriteContext(schema, new java.util.HashMap<>());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(Ticket ticket) {
            recordConsumer.startMessage();
            writeLongField("ENTITY_ID", 0, ticket.getEntityId());
            writeStringField("TITLE", 1, ticket.getTitle());
            writeStringField("TYPE_CD", 2, ticket.getTypeCode());
            writeStringField("STATUS_CD", 3, ticket.getStatusCode());
            writeStringField("PRIORITY_CD", 4, ticket.getPriorityCode());
            writeStringField("COUNSEL_CLAS_CD", 5, ticket.getCounselClasCode());
            writeStringField("COUNSEL_TYPE_LARGE_CD", 6, ticket.getCounselTypeLargeCode());
            writeStringField("COUNSEL_TYPE_MEDIUM_CD", 7, ticket.getCounselTypeMediumCode());
            writeStringField("COUNSEL_TYPE_SMALL_CD", 8, ticket.getCounselTypeSmallCode());
            writeStringField("MEMO", 9, ticket.getMemo());
            writeStringField("CUSTOMER_TYPE", 10, ticket.getCustomerType());
            writeStringField("RESERVATION_TIME", 11, dateToString(ticket.getReservationTime()));  // Date를 String으로 변환
            writeNullableLongField("MANAGER_EID", 12, ticket.getManagerEid());
            writeStringField("ALIMTALK_SEND_YN", 13, ticket.getAlimTalkSendYn());
            writeStringField("ENTITY_STATUS", 14, ticket.getEntityStatus());
            writeStringField("REG_DATE", 15, dateToString(ticket.getRegDate()));  // Date를 String으로 변환
            writeNullableLongField("REG_USER_ENTITY_ID", 16, ticket.getRegUserEntityId());
            writeStringField("MOD_DATE", 17, dateToString(ticket.getModDate()));  // Date를 String으로 변환
            writeNullableLongField("MOD_USER_ENTITY_ID", 18, ticket.getModUserEntityId());
            writeStringField("NPS_UPDATE_YN", 19, ticket.getNpsUpdateYn());
            writeStringField("SUGGESTION_YN", 20, ticket.getSuggestionYn());
            writeStringField("COMPLETE_DATE", 21, dateToString(ticket.getCompleteDate()));  // Date를 String으로 변환
            writeStringField("TEMPLATE_ID", 22, ticket.getTemplateId());
            writeStringField("START_DATE", 23, dateToString(ticket.getStartDate()));  // Date를 String으로 변환
            writeStringField("END_DATE", 24, dateToString(ticket.getEndDate()));  // Date를 String으로 변환
            writeStringField("SEND_DATE", 25, dateToString(ticket.getSendDate()));  // Date를 String으로 변환
            writeStringField("SURVEY_STATUS_CD", 26, ticket.getSurveyStatusCode());
            writeStringField("VOC_TRNS_YN", 27, ticket.getVocTransYn());
            writeStringField("COUNSEL_CATEGORY_CD", 28, ticket.getCounselCateGoryCode());
            writeStringField("TRANSFER_YN", 29, ticket.getTransferYn());
            writeStringField("INIT_QUEUE", 30, ticket.getInitQueue());
            writeStringField("TOBE_QUEUE", 31, ticket.getTobeQueue());
            writeStringField("RESERVE_STAT", 32, ticket.getReserveStat());
            writeNullableLongField("STAT_HISTORY", 33, ticket.getStatHistory());
            writeStringField("RESERVATION_TIME2", 34, dateToString(ticket.getReservationTime2()));  // Date를 String으로 변환
            writeNullableLongField("MODIFY_NUMBER", 35, ticket.getModifyNumber());
            writeNullableLongField("RESERVATION_PERMIT", 36, ticket.getReservationPermit());
            writeStringField("RESERVE_MOD_DATE", 37, dateToString(ticket.getReserveModDate()));  // Date를 String으로 변환
            writeStringField("TRANSFER_TEMPLATE_CONTENT", 38, ticket.getTransferTemplateContent());
            writeStringField("MANAGER_MOD_DATE", 39, dateToString(ticket.getManagerModDate()));  // Date를 String으로 변환
            recordConsumer.endMessage();
        }


        private void writeLongField(String fieldName, int fieldIndex, long value) {
            recordConsumer.startField(fieldName, fieldIndex);
            recordConsumer.addLong(value);
            recordConsumer.endField(fieldName, fieldIndex);
        }

        private void writeStringField(String fieldName, int fieldIndex, String value) {
            if (value != null) {
                recordConsumer.startField(fieldName, fieldIndex);
                recordConsumer.addBinary(Binary.fromString(value));
                recordConsumer.endField(fieldName, fieldIndex);
            }
        }

        private String dateToString(Date date) {
            return date != null ? dateFormat.format(date) : null;
        }

        private void writeNullableLongField(String fieldName, int fieldIndex, Long value) {
            if (value != null) {
                recordConsumer.startField(fieldName, fieldIndex);
                recordConsumer.addLong(value);
                recordConsumer.endField(fieldName, fieldIndex);
            }
        }
    }
}
