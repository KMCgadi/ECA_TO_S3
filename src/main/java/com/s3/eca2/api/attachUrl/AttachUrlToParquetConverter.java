package com.s3.eca2.api.attachUrl;


import com.s3.eca2.domain.attachUrl.AttachUrl;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
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
public class AttachUrlToParquetConverter {

    private static final Logger logger = LoggerFactory.getLogger(AttachUrlToParquetConverter.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(INT64, REQUIRED).named("ATTACH_URL_EID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("LINK_TYPE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("TICKET_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("SHORT_URL"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("SEND_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("SUBMIT_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("LINK_STATUS"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ENTITY_STATUS"))
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, REQUIRED).as(OriginalType.UTF8).named("REG_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("REG_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("MOD_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("MOD_USER_ENTITY_ID"))
            .named("AttachUrl");

    public void writeAttachUrlToParquet(List<AttachUrl> attachUrls, String fileOutputPath) {
        try (ParquetWriter<AttachUrl> writer = new AttachUrlParquetWriter(new Path(fileOutputPath), SCHEMA)) {
            for (AttachUrl attachUrl : attachUrls) {
                writer.write(attachUrl);
            }
        } catch (Exception e) {
            logger.error(String.valueOf(e));
        }
    }

    private static class AttachUrlParquetWriter extends ParquetWriter<AttachUrl> {
        public AttachUrlParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new AttachUrlWriteSupport(schema));
        }
    }

    private static class AttachUrlWriteSupport extends WriteSupport<AttachUrl> {
        private MessageType schema;
        private RecordConsumer recordConsumer;

        public AttachUrlWriteSupport(MessageType schema) {
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
        public void write(AttachUrl attachUrl) {
            recordConsumer.startMessage();
            writeLongField("ATTACH_URL_EID", 0, attachUrl.getAttachUrlEid());
            writeStringField("LINK_TYPE", 1, attachUrl.getLinkType());
            writeNullableLongField("TICKET_ID", 2, attachUrl.getTicketId());
            writeStringField("SHORT_URL", 3, attachUrl.getShortUrl());
            writeStringField("SEND_DATE", 4, dateToString(attachUrl.getSendDate()));
            writeStringField("SUBMIT_DATE", 5, dateToString(attachUrl.getSubmitDate()));
            writeStringField("LINK_STATUS", 6, attachUrl.getLinkStatus());
            writeStringField("ENTITY_STATUS", 7, attachUrl.getEntityStatus());
            writeStringField("REG_DATE", 8, dateToString(attachUrl.getRegDate()));
            writeStringField("REG_USER_ENTITY_ID", 9, attachUrl.getRegUserEntityId());
            writeStringField("MOD_DATE", 10, dateToString(attachUrl.getModDate()));
            writeStringField("MOD_USER_ENTITY_ID", 11, attachUrl.getModUserEntityId());
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
