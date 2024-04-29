package com.s3.eca2.api.counselType;

import org.springframework.stereotype.Component;
import org.apache.parquet.schema.MessageType;
import com.s3.eca2.domain.counselType.CounselType;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

@Component
public class CounselTypeToParquetConverter {
    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(INT64, REQUIRED).named("COUNSEL_TYPE_EID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ENTITY_STATUS"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("MOD_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("REG_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("DEPTH"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("NAME"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("ORDER_NUM"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("CODE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("TEMPLATE_CODE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("PLUS_FRIEND_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("PARENT_CODE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("USABLE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("MOD_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("REG_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("CONTACT_TYPES"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("TICKET_TYPES"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("DESCRIPTION"))
            .named("CounselType");

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger logger = LoggerFactory.getLogger(CounselTypeToParquetConverter.class);

    public void writeCounselTypeToParquet(List<CounselType> counselTypes, String fileOutputPath) throws IOException {
        try (ParquetWriter<CounselType> writer = new CounselTypeParquetWriter(new Path(fileOutputPath), SCHEMA)) {
            for (CounselType counselType : counselTypes) {
                writer.write(counselType);
            }
        } catch (Exception e) {
            logger.error(String.valueOf(e));
        }
    }

    private static class CounselTypeParquetWriter extends ParquetWriter<CounselType> {

        public CounselTypeParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new CounselTypeWriteSupport(schema));
        }
    }

    private static class CounselTypeWriteSupport extends WriteSupport<CounselType> {
        private MessageType schema;
        private RecordConsumer recordConsumer;

        public CounselTypeWriteSupport(MessageType schema) {
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
        public void write(CounselType counselType) {
            recordConsumer.startMessage();
            writeLongField("COUNSEL_TYPE_EID", 0, counselType.getCounselTypeEid());
            writeStringField("ENTITY_STATUS", 1, counselType.getEntityStatus());
            writeStringField("MOD_DATE", 2, dateToString(counselType.getModDate()));
            writeStringField("REG_DATE", 3, dateToString(counselType.getRegDate()));
            writeNullableLongField("DEPTH", 4, counselType.getDepth());
            writeStringField("NAME", 5, counselType.getName());
            writeNullableLongField("ORDER_NUM", 6, counselType.getOrderNum());
            writeStringField("CODE", 7, counselType.getCode());
            writeStringField("TEMPLATE_CODE", 8, counselType.getTemplateCode());
            writeStringField("PLUS_FRIEND_ID", 9, counselType.getPlusFriendId());
            writeStringField("PARENT_CODE", 10, counselType.getParentCode());
            writeStringField("USABLE", 11, counselType.getUsable());
            writeNullableLongField("MOD_USER_ENTITY_ID", 12, counselType.getModUserEntityId());
            writeNullableLongField("REG_USER_ENTITY_ID", 13, counselType.getRegUserEntityId());
            writeStringField("CONTACT_TYPES", 14, counselType.getContactTypes());
            writeStringField("TICKET_TYPES", 15, counselType.getTicketTypes());
            writeStringField("DESCRIPTION", 16, counselType.getDescription());
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
