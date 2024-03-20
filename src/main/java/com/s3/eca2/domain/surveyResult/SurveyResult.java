package com.s3.eca2.domain.surveyResult;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


import javax.persistence.*;
import java.util.Date;
@Getter
@Setter
@ToString
@Entity
@Table(name = "ECA_CS_SURVEY_RESULT_TM")
public class SurveyResult {
    @Id
    @Column(name = "SURVEY_ENTITY_ID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long surveyEntityId;

    @Column(name = "TICKET_EID")
    private long ticketEid;

    @Column(name = "TEMPLATE_ID")
    private String templateId;

    @Column(name = "TEMPLATE_TITLE")
    private String templateTitle;

    @Column(name = "TOKEN")
    private String token;

    @Column(name = "QUESTION_01")
    private String question01;

    @Column(name = "QUESTION_02")
    private String question02;

    @Column(name = "QUESTION_03")
    private String question03;

    @Column(name = "QUESTION_04")
    private String question04;

    @Column(name = "QUESTION_05")
    private String question05;

    @Column(name = "QUESTION_06")
    private String question06;

    @Column(name = "QUESTION_07")
    private String question07;

    @Column(name = "QUESTION_08")
    private String question08;

    @Column(name = "QUESTION_09")
    private String question09;

    @Column(name = "QUESTION_10")
    private String question10;

    @Column(name = "ANSWER_01")
    private String answer01;

    @Column(name = "ANSWER_02")
    private String answer02;

    @Column(name = "ANSWER_03")
    private String answer03;

    @Column(name = "ANSWER_04")
    private String answer04;

    @Column(name = "ANSWER_05")
    private String answer05;

    @Column(name = "ANSWER_06")
    private String answer06;

    @Column(name = "ANSWER_07")
    private String answer07;

    @Column(name = "ANSWER_08")
    private String answer08;

    @Column(name = "ANSWER_09")
    private String answer09;

    @Column(name = "ANSWER_10")
    private String answer10;

    @Column(name = "SEND_DATE")
    private Date sendDate;

    @Column(name = "RESPONSE_DATE")
    private Date responseDate;

    @Column(name = "ENTITY_STATUS")
    private String entityStatus;

    @Column(name = "MOD_DATE")
    private Date modDate;

    @Column(name = "REG_DATE")
    private Date regDate;

    @Column(name = "MOD_USER_ENTITY_ID")
    private Long modUserEntityId;

    @Column(name = "REG_USER_ENTITY_ID")
    private Long regUserEntityId;

    @Column(name = "COUNSEL_TYPE_LARGE_CD")
    private String counselTypeLargeCode;

    @Column(name = "COUNSEL_TYPE_MEDIUM_CD")
    private String counselTypeMediumCode;

    @Column(name = "COUNSEL_TYPE_SMALL_CD")
    private String counselTypeSmallCode;

    @Column(name = "CONTACT_CD")
    private String contactCode;

    @Column(name = "MANAGER_EID")
    private Long managerEid;
}
