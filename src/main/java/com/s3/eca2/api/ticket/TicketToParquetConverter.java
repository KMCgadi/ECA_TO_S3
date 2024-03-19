package com.s3.eca2.api.ticket;

import com.s3.eca2.domain.ticket.Ticket;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.Types;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

@Component
public class TicketToParquetConverter {

    private static final MessageType SCHEMA = Types.buildMessage()
            // 문자열 필드
            .addField(Types.primitive(BINARY, OPTIONAL).named("title"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("typeCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("statusCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("priorityCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("counselClasCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("counselTypeLargeCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("counselTypeMediumCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("counselTypeSmallCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("memo"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("customerType"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("alimTalkSendYn"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("entityStatus"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("npsUpdateYn"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("suggestionYn"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("templateId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("surveyStatusCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("vocTransYn"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("counselCateGoryCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("initQueue"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("tobeQueue"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("reserveStat"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("transferTemplateContent"))
            // 숫자 필드
            .addField(Types.primitive(BINARY, OPTIONAL).named("entityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("managerId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modifyNumber"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("reservationPermit"))
            // 날짜 필드
            .addField(Types.primitive(BINARY, OPTIONAL).named("reservationTime"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("completeDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("startDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("endDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("sendDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("reservationTime2"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("reserveModDate"))

            .named("Ticket");
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeTicketsToParquet(List<Ticket> tickets, String fileOutputPath) throws IOException {
        try (ParquetWriter<Ticket> writer = new TicketParquetWriter(new Path(fileOutputPath), SCHEMA)) {
            for (Ticket ticket : tickets) {
                writer.write(ticket);
            }
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
            writeStringField("title", ticket.getTitle());
            writeStringField("typeCode", ticket.getTypeCode());
            writeStringField("statusCode", ticket.getStatusCode());
            writeStringField("priorityCode", ticket.getPriorityCode());
            writeStringField("counselClasCode", ticket.getCounselClasCode());
            writeStringField("counselTypeLargeCode", ticket.getCounselTypeLargeCode());
            writeStringField("counselTypeMediumCode", ticket.getCounselTypeMediumCode());
            writeStringField("counselTypeSmallCode", ticket.getCounselTypeSmallCode());
            writeStringField("memo", ticket.getMemo());
            writeStringField("customerType", ticket.getCustomerType());
            writeStringField("alimTalkSendYn", ticket.getAlimTalkSendYn());
            writeStringField("entityStatus", ticket.getEntityStatus());
            writeStringField("npsUpdateYn", ticket.getNpsUpdateYn());
            writeStringField("suggestionYn", ticket.getSuggestionYn());
            writeStringField("templateId", ticket.getTemplateId());
            writeStringField("surveyStatusCode", ticket.getSurveyStatusCode());
            writeStringField("vocTransYn", ticket.getVocTransYn());
            writeStringField("counselCateGoryCode", ticket.getCounselCateGoryCode());
            writeStringField("initQueue", ticket.getInitQueue());
            writeStringField("tobeQueue", ticket.getTobeQueue());
            writeStringField("reserveStat", ticket.getReserveStat());
            writeStringField("transferTemplateContent", ticket.getTransferTemplateContent());

            // 숫자 필드는 문자열로 변환하여 기록
            writeOptionalStringField("entityId", ticket.getEntityId());
            writeOptionalStringField("managerId", ticket.getManagerId());
            writeOptionalStringField("regUserEntityId", ticket.getRegUserEntityId());
            writeOptionalStringField("modUserEntityId", ticket.getModUserEntityId());
            writeOptionalStringField("modifyNumber", ticket.getModifyNumber());
            writeOptionalStringField("reservationPermit", ticket.getReservationPermit());

            // 날짜 필드는 문자열로 변환하여 기록
            writeOptionalDateStringField("reservationTime", ticket.getReservationTime());
            writeOptionalDateStringField("regDate", ticket.getRegDate());
            writeOptionalDateStringField("modDate", ticket.getModDate());
            writeOptionalDateStringField("completeDate", ticket.getCompleteDate());
            writeOptionalDateStringField("startDate", ticket.getStartDate());
            writeOptionalDateStringField("endDate", ticket.getEndDate());
            writeOptionalDateStringField("sendDate", ticket.getSendDate());
            writeOptionalDateStringField("reservationTime2", ticket.getReservationTime2());
            writeOptionalDateStringField("reserveModDate", ticket.getReserveModDate());

            recordConsumer.endMessage();
        }

        private void writeStringField(String fieldName, String value) {
            if (value != null) {
                int fieldIndex = schema.getFieldIndex(fieldName);
                recordConsumer.startField(fieldName, fieldIndex);
                recordConsumer.addBinary(Binary.fromString(value));
                recordConsumer.endField(fieldName, fieldIndex);
            }
        }

        private void writeOptionalStringField(String fieldName, Object value) {
            if (value != null) {
                writeStringField(fieldName, value.toString());
            }
        }

        private void writeOptionalDateStringField(String fieldName, Date date) {
            if (date != null) {
                writeStringField(fieldName, dateFormat.format(date));
            }
        }
    }
}
