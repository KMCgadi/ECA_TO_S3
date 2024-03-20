package com.s3.eca2.api.organizationType;

import com.s3.eca2.domain.organizationType.OrganizationType;
import org.springframework.stereotype.Component;
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
public class OrganizationToParquetConverter {
    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(BINARY, OPTIONAL).named("organizationTypeEid"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("entityStatus"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("code"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("parentCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("depth"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("name"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("orderNum"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("companyCode"))
            .named("OrganizationType");

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeOrganizationTypeToParquet(List<OrganizationType> organizationTypes, String fileOutputPath) throws IOException {
        try (ParquetWriter<OrganizationType> writer = new OrganizationTypeParquetWriter(new Path(fileOutputPath), SCHEMA)) {
            for (OrganizationType organizationType : organizationTypes) {
                writer.write(organizationType);
            }
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
            writeOptionalStringField("organizationTypeEid", organizationType.getOrganizationTypeEid());
            writeStringField("entityStatus", organizationType.getEntityStatus());
            writeOptionalDateStringField("modDate", organizationType.getModDate());
            writeOptionalDateStringField("regDate", organizationType.getRegDate());
            writeStringField("code", organizationType.getCode());
            writeStringField("parentCode", organizationType.getParentCode());
            writeOptionalStringField("depth", organizationType.getDepth());
            writeStringField("name", organizationType.getName());
            writeOptionalStringField("orderNum", organizationType.getOrderNum());
            writeOptionalStringField("modUserEntityId", organizationType.getModUserEntityId());
            writeOptionalStringField("regUserEntityId", organizationType.getRegUserEntityId());
            writeOptionalStringField("companyCode", organizationType.getCompanyCode());
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
