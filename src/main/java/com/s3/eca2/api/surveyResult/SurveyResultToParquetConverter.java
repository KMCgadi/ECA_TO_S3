package com.s3.eca2.api.surveyResult;

import com.s3.eca2.domain.surveyResult.SurveyResult;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.simple.Primitive;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;

@Component
public class SurveyResultToParquetConverter {

    private static final Logger logger = LoggerFactory.getLogger(SurveyResultToParquetConverter.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(BINARY, OPTIONAL).named("surveyEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("ticketEid"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("templateId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("templateTitle"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("token"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("question01"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("question02"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("question03"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("question04"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("question05"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("question06"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("question07"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("question08"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("question09"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("question10"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("answer01"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("answer02"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("answer03"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("answer04"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("answer05"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("answer06"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("answer07"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("answer08"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("answer09"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("answer10"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("sendDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("responseDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("entityStatus"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("counselTypeLargeCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("counselTypeMediumCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("counselTypeSmallCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("contactCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("managerEid"))
            .named("SurveyResult");

    public void writeSurveyResultToParquet(List<SurveyResult> surveyResults, String fileOutputPath) throws IOException {
        try (ParquetWriter<SurveyResult> writer = new SurveyResultParquetWriter(new Path(fileOutputPath), SCHEMA)) {
            for (SurveyResult surveyResult : surveyResults) {
                logger.debug("Writing surveyResult: {}", surveyResult);
                writer.write(surveyResult);
            }
        } catch (Exception e) {
            logger.error("Error writing Parquet file: ", e);
            throw e;
        }
    }

    private static class SurveyResultParquetWriter extends ParquetWriter<SurveyResult> {
        public SurveyResultParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new SurveyResultWriteSupport(schema));
        }
    }

    private static class SurveyResultWriteSupport extends WriteSupport<SurveyResult> {
        private MessageType schema;
        private RecordConsumer recordConsumer;

        public SurveyResultWriteSupport(MessageType schema) {
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
        public void write(SurveyResult surveyResult) {
            recordConsumer.startMessage();
            writeOptionalStringField("surveyEntityId", String.valueOf(surveyResult.getSurveyEntityId()));
            writeOptionalStringField("ticketEid", String.valueOf(surveyResult.getTicketEid()));
            writeStringField("templateId", surveyResult.getTemplateId());
            writeStringField("templateTitle", surveyResult.getTemplateTitle());
            writeStringField("token", surveyResult.getToken());
            writeStringField("question01", surveyResult.getQuestion01());
            writeStringField("question02", surveyResult.getQuestion02());
            writeStringField("question03", surveyResult.getQuestion03());
            writeStringField("question04", surveyResult.getQuestion04());
            writeStringField("question05", surveyResult.getQuestion05());
            writeStringField("question06", surveyResult.getQuestion06());
            writeStringField("question07", surveyResult.getQuestion07());
            writeStringField("question08", surveyResult.getQuestion08());
            writeStringField("question09", surveyResult.getQuestion09());
            writeStringField("question10", surveyResult.getQuestion10());
            writeStringField("answer01", surveyResult.getAnswer01());
            writeStringField("answer02", surveyResult.getAnswer02());
            writeStringField("answer03", surveyResult.getAnswer03());
            writeStringField("answer04", surveyResult.getAnswer04());
            writeStringField("answer05", surveyResult.getAnswer05());
            writeStringField("answer06", surveyResult.getAnswer06());
            writeStringField("answer07", surveyResult.getAnswer07());
            writeStringField("answer08", surveyResult.getAnswer08());
            writeStringField("answer09", surveyResult.getAnswer09());
            writeStringField("answer10", surveyResult.getAnswer10());
            writeOptionalDateStringField("sendDate", surveyResult.getSendDate());
            writeOptionalDateStringField("responseDate", surveyResult.getResponseDate());
            writeStringField("entityStatus", surveyResult.getEntityStatus());
            writeOptionalDateStringField("modDate", surveyResult.getModDate());
            writeOptionalDateStringField("regDate", surveyResult.getRegDate());
            writeOptionalStringField("modUserEntityId", String.valueOf(surveyResult.getModUserEntityId()));
            writeOptionalStringField("regUserEntityId", String.valueOf(surveyResult.getRegUserEntityId()));
            writeStringField("counselTypeLargeCode", surveyResult.getCounselTypeLargeCode());
            writeStringField("counselTypeMediumCode", surveyResult.getCounselTypeMediumCode());
            writeStringField("counselTypeSmallCode", surveyResult.getCounselTypeSmallCode());
            writeStringField("contactCode", surveyResult.getContactCode());
            writeOptionalStringField("managerEid", String.valueOf(surveyResult.getManagerEid()));
            recordConsumer.endMessage();
        }

        private void writeOptionalStringField(String fieldName, Object value) {
            if (value != null) {
                int fieldIndex = schema.getFieldIndex(fieldName);
                recordConsumer.startField(fieldName, fieldIndex);
                recordConsumer.addBinary(Binary.fromString(value.toString()));
                recordConsumer.endField(fieldName, fieldIndex);
            }
        }

        private void writeStringField(String fieldName, String value) {
            if (value != null) {
                int fieldIndex = schema.getFieldIndex(fieldName);
                recordConsumer.startField(fieldName, fieldIndex);
                recordConsumer.addBinary(Binary.fromString(value));
                recordConsumer.endField(fieldName, fieldIndex);
            } else {
                logger.debug("Field {} is null.", fieldName);
            }
        }

        private void writeOptionalDateStringField(String fieldName, Date date) {
            if (date != null) {
                writeStringField(fieldName, dateFormat.format(date));
            } else {
                logger.debug("Field {} is null.", fieldName);
            }
        }
    }
}
