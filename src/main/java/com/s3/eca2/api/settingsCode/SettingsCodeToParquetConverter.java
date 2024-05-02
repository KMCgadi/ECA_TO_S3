package com.s3.eca2.api.settingsCode;

import com.s3.eca2.domain.settingsCode.SettingsCode;
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
public class SettingsCodeToParquetConverter {
    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(INT64, REQUIRED).named("ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ENTITY_STATUS"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("MOD_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("REG_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("PATH"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("CODE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("NAME"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("ORDER_NUM"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("REMARK_TEXT"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("USE_TYPE_CODE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("MOD_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("REG_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("PARENT_EID"))
            .named("SettingsCode");

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger logger = LoggerFactory.getLogger(SettingsCodeToParquetConverter.class);

    public void writeSettingsCodeToParquet(List<SettingsCode> settingsCodes, String fileOutputPath) throws IOException {
        try (ParquetWriter<SettingsCode> writer = new SettingsCodeParquetWriter(new Path(fileOutputPath), SCHEMA)) {
            for (SettingsCode settingsCode : settingsCodes) {
                writer.write(settingsCode);
            }
        } catch (Exception e) {
            logger.error(String.valueOf(e));
        }
    }

    private static class SettingsCodeParquetWriter extends ParquetWriter<SettingsCode> {

        public SettingsCodeParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new SettingsCodeWriteSupport(schema));
        }
    }

    private static class SettingsCodeWriteSupport extends WriteSupport<SettingsCode> {
        private MessageType schema;
        private RecordConsumer recordConsumer;

        public SettingsCodeWriteSupport(MessageType schema) {
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
        public void write(SettingsCode settingsCode) {
            recordConsumer.startMessage();
            writeLongField("ENTITY_ID", 0, settingsCode.getEntityId());
            writeStringField("ENTITY_STATUS", 1, settingsCode.getEntityStatus());
            writeStringField("MOD_DATE", 2, dateToString(settingsCode.getModDate()));
            writeStringField("REG_DATE", 3, dateToString(settingsCode.getRegDate()));
            writeStringField("PATH", 4, settingsCode.getPath());
            writeStringField("CODE", 5, settingsCode.getCode());
            writeStringField("NAME", 6, settingsCode.getName());
            writeLongField("ORDER_NUM", 7, settingsCode.getOrderNum());
            writeStringField("REMARK_TEXT", 8, settingsCode.getRemarkText());
            writeStringField("USE_TYPE_CODE", 9, settingsCode.getUseTypeCode());
            writeNullableLongField("MOD_USER_ENTITY_ID", 10, settingsCode.getModUserEntityId());
            writeNullableLongField("REG_USER_ENTITY_ID", 11, settingsCode.getRegUserEntityId());
            if (settingsCode.getParentCode() != null) {
                writeLongField("PARENT_EID", 12, settingsCode.getParentCode().getEntityId());
            }
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
