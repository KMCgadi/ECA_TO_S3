package com.s3.eca2.domain.ticket;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;
import java.util.Date;

@Getter
@Setter
@ToString
@Entity
@Table(name = "ECA_CS_TICKET_TM")
public class Ticket {
    @Id
    @Column(name = "ENTITY_ID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long entityId;

    @Column(name = "TITLE")
    private String title;

    @Column(name = "TYPE_CD")
    private String typeCode;

    @Column(name = "STATUS_CD")
    private String statusCode;

    @Column(name = "PRIORITY_CD")
    private String priorityCode;

    @Column(name = "COUNSEL_CLAS_CD")
    private String counselClasCode;

    @Column(name = "COUNSEL_TYPE_LARGE_CD")
    private String counselTypeLargeCode;

    @Column(name = "COUNSEL_TYPE_MEDIUM_CD")
    private String counselTypeMediumCode;

    @Column(name = "COUNSEL_TYPE_SMALL_CD")
    private String counselTypeSmallCode;

    @Column(name = "MEMO")
    private String memo;

    @Column(name = "CUSTOMER_TYPE")
    private String customerType;

    @Column(name = "RESERVATION_TIME")
    private Date reservationTime;

    @Column(name = "MANAGER_EID")
    private Long managerEid;

    @Column(name = "ALIMTALK_SEND_YN")
    private String alimTalkSendYn;

    @Column(name = "ENTITY_STATUS")
    private String entityStatus;

    @Column(name = "REG_DATE")
    private Date regDate;

    @Column(name = "REG_USER_ENTITY_ID")
    private Long regUserEntityId;

    @Column(name = "MOD_DATE")
    private Date modDate;

    @Column(name = "MOD_USER_ENTITY_ID")
    private Long modUserEntityId;

    @Column(name = "NPS_UPDATE_YN")
    private String npsUpdateYn;

    @Column(name = "SUGGESTION_YN")
    private String suggestionYn;

    @Column(name = "COMPLETE_DATE")
    private Date completeDate;

    @Column(name = "TEMPLATE_ID")
    private String templateId;

    @Column(name = "START_DATE")
    private Date startDate;

    @Column(name = "END_DATE")
    private Date endDate;

    @Column(name = "SEND_DATE")
    private Date sendDate;

    @Column(name = "SURVEY_STATUS_CD")
    private String surveyStatusCode;

    @Column(name = "VOC_TRNS_YN")
    private String vocTransYn;

    @Column(name = "COUNSEL_CATEGORY_CD")
    private String counselCateGoryCode;

    @Column(name = "TRANSFER_YN")
    private String transferYn;

    @Column(name = "INIT_QUEUE")
    private String initQueue;

    @Column(name = "TOBE_QUEUE")
    private String tobeQueue;

    @Column(name = "RESERVE_STAT")
    private String reserveStat;

    @Column(name = "STAT_HISOTRY")
    private Long statHistory;

    @Column(name = "RESERVATION_TIME2")
    private Date reservationTime2;

    @Column(name = "MODIFY_NUMBER")
    private Long modifyNumber;

    @Column(name = "RESERVATION_PERMIT")
    private Long reservationPermit;

    @Column(name = "RESERVE_MOD_DATE")
    private Date reserveModDate;

    @Column(name = "TRANSFER_TEMPLATE_CONTENT")
    private String transferTemplateContent;

    @Column(name = "MANAGER_MOD_DATE")
    private Date managerModDate;
}
