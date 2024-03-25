package com.s3.eca2.api.organizationType;

import com.s3.eca2.domain.organizationType.OrganizationType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

@Component
public class OrganizationToParquetConverter {
    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(INT64, REQUIRED).named("ORGANIZATION_TYPE_EID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ENTITY_STATUS"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("MOD_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("REG_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("CODE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("PARENT_CODE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("DEPTH"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("NAME"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("ORDER_NUM"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("MOD_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("REG_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("COMPANYCODE"))
            .named("OrganizationType");

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger logger = LoggerFactory.getLogger(OrganizationToParquetConverter.class);

    public void writeOrganizationTypeToParquet(List<OrganizationType> organizationTypes, String fileOutputPath) throws IOException {
        try (ParquetWriter<OrganizationType> writer = new OrganizationTypeParquetWriter(new Path(fileOutputPath), SCHEMA)) {
            for (OrganizationType organizationType : organizationTypes) {
                writer.write(organizationType);
            }
        } catch (Exception e) {
            logger.error(String.valueOf(e));
        }
    }

    private static class OrganizationTypeParquetWriter extends ParquetWriter<OrganizationType> {

        public OrganizationTypeParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new OrganizationTypeWriteSupport(schema));
        }
    }

    private static class OrganizationTypeWriteSupport extends WriteSupport<OrganizationType> {
        private MessageType schema;
        private RecordConsumer recordConsumer;

        public OrganizationTypeWriteSupport(MessageType schema) {
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
        public void write(OrganizationType organizationType) {
            recordConsumer.startMessage();
            writeLongField("ORGANIZATION_TYPE_EID", 0, organizationType.getOrganizationTypeEid());
            writeStringField("ENTITY_STATUS", 1, organizationType.getEntityStatus());
            writeStringField("MOD_DATE", 2, dateToString(organizationType.getModDate()));
            writeStringField("REG_DATE", 3, dateToString(organizationType.getRegDate()));
            writeStringField("CODE", 4, organizationType.getCode());
            writeStringField("PARENT_CODE", 5, organizationType.getParentCode());
            writeNullableLongField("DEPTH", 6, organizationType.getDepth());
            writeStringField("NAME", 7, organizationType.getName());
            writeNullableLongField("ORDER_NUM", 8, organizationType.getOrderNum());
            writeNullableLongField("MOD_USER_ENTITY_ID", 9, organizationType.getModUserEntityId());
            writeNullableLongField("REG_USER_ENTITY_ID", 10, organizationType.getRegUserEntityId());
            writeNullableLongField("COMPANYCODE", 11, organizationType.getCompanyCode());
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
