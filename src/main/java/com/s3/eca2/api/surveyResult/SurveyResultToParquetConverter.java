package com.s3.eca2.api.surveyResult;

import com.s3.eca2.domain.surveyResult.SurveyResult;
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
public class SurveyResultToParquetConverter {

    private static final Logger logger = LoggerFactory.getLogger(SurveyResultToParquetConverter.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(INT64, REQUIRED).named("SURVEY_ENTITY_ID"))
            .addField(Types.primitive(INT64, REQUIRED).named("TICKET_EID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("TEMPLATE_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("TEMPLATE_TITLE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("TOKEN"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("QUESTION_01"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("QUESTION_02"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("QUESTION_03"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("QUESTION_04"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("QUESTION_05"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("QUESTION_06"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("QUESTION_07"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("QUESTION_08"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("QUESTION_09"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("QUESTION_10"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ANSWER_01"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ANSWER_02"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ANSWER_03"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ANSWER_04"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ANSWER_05"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ANSWER_06"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ANSWER_07"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ANSWER_08"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ANSWER_09"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ANSWER_10"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("SEND_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("RESPONSE_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ENTITY_STATUS"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("MOD_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("REG_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("MOD_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("REG_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("COUNSEL_TYPE_LARGE_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("COUNSEL_TYPE_MEDIUM_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("COUNSEL_TYPE_SMALL_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("CONTACT_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("MANAGER_EID"))
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

            writeLongField("SURVEY_ENTITY_ID", 0, surveyResult.getSurveyEntityId());
            writeLongField("TICKET_EID", 1, surveyResult.getTicketEid());
            writeStringField("TEMPLATE_ID", 2, surveyResult.getTemplateId());
            writeStringField("TEMPLATE_TITLE", 3, surveyResult.getTemplateTitle());
            writeStringField("TOKEN", 4, surveyResult.getToken());
            writeStringField("QUESTION_01", 5, surveyResult.getQuestion01());
            writeStringField("QUESTION_02", 6, surveyResult.getQuestion02());
            writeStringField("QUESTION_03", 7, surveyResult.getQuestion03());
            writeStringField("QUESTION_04", 8, surveyResult.getQuestion04());
            writeStringField("QUESTION_05", 9, surveyResult.getQuestion05());
            writeStringField("QUESTION_06", 10, surveyResult.getQuestion06());
            writeStringField("QUESTION_07", 11, surveyResult.getQuestion07());
            writeStringField("QUESTION_08", 12, surveyResult.getQuestion08());
            writeStringField("QUESTION_09", 13, surveyResult.getQuestion09());
            writeStringField("QUESTION_10", 14, surveyResult.getQuestion10());
            writeStringField("ANSWER_01", 15, surveyResult.getAnswer01());
            writeStringField("ANSWER_02", 16, surveyResult.getAnswer02());
            writeStringField("ANSWER_03", 17, surveyResult.getAnswer03());
            writeStringField("ANSWER_04", 18, surveyResult.getAnswer04());
            writeStringField("ANSWER_05", 19, surveyResult.getAnswer05());
            writeStringField("ANSWER_06", 20, surveyResult.getAnswer06());
            writeStringField("ANSWER_07", 21, surveyResult.getAnswer07());
            writeStringField("ANSWER_08", 22, surveyResult.getAnswer08());
            writeStringField("ANSWER_09", 23, surveyResult.getAnswer09());
            writeStringField("ANSWER_10", 24, surveyResult.getAnswer10());
            writeStringField("SEND_DATE", 25, dateToString(surveyResult.getSendDate()));
            writeStringField("RESPONSE_DATE", 26, dateToString(surveyResult.getResponseDate()));
            writeStringField("ENTITY_STATUS", 27, surveyResult.getEntityStatus());
            writeStringField("MOD_DATE", 28, dateToString(surveyResult.getModDate()));
            writeStringField("REG_DATE", 29, dateToString(surveyResult.getRegDate()));
            writeNullableLongField("MOD_USER_ENTITY_ID", 30, surveyResult.getModUserEntityId());
            writeNullableLongField("REG_USER_ENTITY_ID", 31, surveyResult.getRegUserEntityId());
            writeStringField("COUNSEL_TYPE_LARGE_CD", 32, surveyResult.getCounselTypeLargeCode());
            writeStringField("COUNSEL_TYPE_MEDIUM_CD", 33, surveyResult.getCounselTypeMediumCode());
            writeStringField("COUNSEL_TYPE_SMALL_CD", 34, surveyResult.getCounselTypeSmallCode());
            writeStringField("CONTACT_CD", 35, surveyResult.getContactCode());
            writeNullableLongField("MANAGER_EID", 36, surveyResult.getManagerEid());
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

        private void writeDateField(String fieldName, int fieldIndex, Date date) {
            if (date != null) {
                long timeInMillis = date.getTime(); // 날짜를 밀리초로 변환
                recordConsumer.startField(fieldName, fieldIndex);
                recordConsumer.addLong(timeInMillis); // TIMESTAMP_MILLIS 타입으로 기록
                recordConsumer.endField(fieldName, fieldIndex);
            }
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
