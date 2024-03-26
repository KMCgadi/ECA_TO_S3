package com.s3.eca2.api.user;

import com.s3.eca2.domain.user.User;
import org.apache.hadoop.fs.Path;
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

import org.apache.parquet.schema.OriginalType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

@Component
public class UserToParquetConverter {
    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(INT64, REQUIRED).named("USER_EID"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("ENTITY_STATUS"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("MOD_DATE"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("REG_DATE"))
            .addField(Types.optional(INT64).named("AUTH_CODE"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("BIRTH_DAY"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("CENTER_CD"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("DEPT_CD"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("EMAIL"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("FAX"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("HIRE_DATE"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("HOME_ADDR"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("HOME_TEL"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("ID"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("LAST_EDU_CD"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("LEAVE_DATE"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("MOBILE"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("NAME"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("NOTE"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("PASSWORD"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("POSITION_CD"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("RANK_CD"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("RESIGN_REASON"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("TEAM_CD"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("UNUSED_REASON_CODE"))
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, REQUIRED).as(OriginalType.UTF8).named("USABLE"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("WEDDING_DATE"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("CTI_EXTENSION"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("CTI_LOGIN_ID"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("CTI_YN"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("IP_ADDR"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("WORK_STATE_CD"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("CONTRACT_STATE_CD"))
            .addField(Types.optional(INT64).named("MOD_USER_ENTITY_ID"))
            .addField(Types.optional(INT64).named("REG_USER_ENTITY_ID"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("TOKEN"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("REC_MANAGE_YN"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("REC_COUNSEL_YN"))
            .named("User");

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger logger = LoggerFactory.getLogger(UserToParquetConverter.class);

    public void writeUserToParquet(List<User> users, String fileOutputPath) throws IOException {
        try (ParquetWriter<User> writer = new UserToParquetConverter.UserParquetWriter(new Path(fileOutputPath),SCHEMA)) {
            for (User user : users) {
                writer.write(user);
            }
        } catch (Exception e) {
            logger.error(String.valueOf(e));
        }
    }

    private static class UserParquetWriter extends ParquetWriter<User> {
        public UserParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new UserToParquetConverter.UserWriteSupport(schema));
        }
    }

    private static class UserWriteSupport extends WriteSupport<User> {
        private MessageType schema;

        private RecordConsumer recordConsumer;

        public UserWriteSupport(MessageType schema){this.schema = schema;}

        @Override
        public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
            return new WriteContext(schema, new java.util.HashMap<>());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(User user) {
            recordConsumer.startMessage();
            writeLongField("USER_EID", 0, user.getUserEid());
            writeStringField("ENTITY_STATUS", 1, user.getEntityStatus());
            writeStringField("MOD_DATE", 2, dateToString(user.getModDate()));
            writeStringField("REG_DATE", 3, dateToString(user.getRegDate()));
            writeNullableLongField("AUTH_CODE", 4, user.getAuthCode());
            writeStringField("BIRTH_DAY", 5, dateToString(user.getBirthDay()));
            writeStringField("CENTER_CD", 6, user.getCenterCode());
            writeStringField("DEPT_CD", 7, user.getDeptCode());
            writeStringField("EMAIL", 8, user.getEmail());
            writeStringField("FAX", 9, user.getFax());
            writeStringField("HIRE_DATE", 10, dateToString(user.getHireDate()));
            writeStringField("HOME_ADDR", 11, user.getHomeAddr());
            writeStringField("HOME_TEL", 12, user.getHomeTel());
            writeStringField("ID", 13, user.getId());
            writeStringField("LAST_EDU_CD", 14, user.getLastEduCode());
            writeStringField("LEAVE_DATE", 15, dateToString(user.getLeaveDate()));
            writeStringField("MOBILE", 16, user.getMobile());
            writeStringField("NAME", 17, user.getName());
            writeStringField("NOTE", 18, user.getNote());
            writeStringField("PASSWORD", 19, user.getPassword());
            writeStringField("POSITION_CD", 20, user.getPositionCode());
            writeStringField("RANK_CD", 21, user.getRankCode());
            writeStringField("RESIGN_REASON", 22, user.getResignReason());
            writeStringField("TEAM_CD", 23, user.getTeamCode());
            writeStringField("UNUSED_REASON_CODE", 24, user.getUnusedReasonCode());
            writeStringField("USABLE", 25, user.getUsable());
            writeStringField("WEDDING_DATE", 26, dateToString(user.getWeddingDate()));
            writeStringField("CTI_EXTENSION", 27, user.getCtiExtension());
            writeStringField("CTI_LOGIN_ID", 28, user.getCtiLoginId());
            writeStringField("CTI_YN", 29, user.getCtiYn());
            writeStringField("IP_ADDR", 30, user.getIpAddr());
            writeStringField("WORK_STATE_CD", 31, user.getWorkStateCode());
            writeStringField("CONTRACT_STATE_CD", 32, user.getContractStateCode());
            writeNullableLongField("MOD_USER_ENTITY_ID", 33, user.getModUserEntityId());
            writeNullableLongField("REG_USER_ENTITY_ID", 34, user.getRegUserEntityId());
            writeStringField("TOKEN", 35, user.getToken());
            writeStringField("REC_MANAGE_YN", 36, user.getRecManageYn());
            writeStringField("REC_COUNSEL_YN", 37, user.getRecCounselYn());
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
