package com.s3.eca2.domain.user;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


import javax.persistence.*;
import java.util.Date;


@Getter
@Setter
@ToString
@Entity
@Table(name = "GAEA_USER_TM")
public class User {
    @Id
    @Column(name = "USER_EID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long userEid;

    @Column(name = "ENTITY_STATUS")
    private String entityStatus;

    @Column(name = "MOD_DATE")
    private Date modDate;

    @Column(name = "REG_DATE")
    private Date regDate;

    @Column(name = "AUTH_CODE")
    private Long authCode;

    @Column(name = "BIRTH_DAY")
    private Date birthDay;

    @Column(name = "CENTER_CD")
    private String centerCode;

    @Column(name = "DEPT_CD")
    private String deptCode;

    @Column(name = "EMAIL")
    private String email;

    @Column(name = "FAX")
    private String fax;

    @Column(name = "HIRE_DATE")
    private Date hireDate;

    @Column(name = "HOME_ADDR")
    private String homeAddr;

    @Column(name = "HOME_TEL")
    private String homeTel;

    @Column(name = "ID")
    private String id;

    @Column(name = "LAST_EDU_CD")
    private String lastEduCode;

    @Column(name = "LEAVE_DATE")
    private Date leaveDate;

    @Column(name = "MOBILE")
    private String mobile;

    @Column(name = "NAME")
    private String name;

    @Column(name = "NOTE")
    private String note;

    @Column(name = "PASSWORD")
    private String password;

    @Column(name = "POSITION_CD")
    private String positionCode;

    @Column(name = "RANK_CD")
    private String rankCode;

    @Column(name = "RESIGN_REASON")
    private String resignReason;

    @Column(name = "TEAM_CD")
    private String teamCode;

    @Column(name = "UNUSED_REASON_CODE")
    private String unusedReasonCode;

    @Column(name = "USABLE")
    private String usable;

    @Column(name = "WEDDING_DATE")
    private Date weddingDate;

    @Column(name = "CTI_EXTENSION")
    private String ctiExtension;

    @Column(name = "CTI_LOGIN_ID")
    private String ctiLoginId;

    @Column(name = "CTI_YN")
    private String ctiYn;

    @Column(name = "IP_ADDR")
    private String ipAddr;

    @Column(name = "WORK_STATE_CD")
    private String workStateCode;

    @Column(name = "CONTRACT_STATE_CD")
    private String contractStateCode;

    @Column(name = "MOD_USER_ENTITY_ID")
    private Long modUserEntityId;

    @Column(name = "REG_USER_ENTITY_ID")
    private Long regUserEntityId;

    @Column(name = "TOKEN")
    private String token;

    @Column(name = "REC_MANAGE_YN")
    private String recManageYn;

    @Column(name = "REC_COUNSEL_YN")
    private String recCounselYn;
}
