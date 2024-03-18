package com.s3.eca2.domain.toastHistory;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


import javax.persistence.*;
import java.util.Date;
@Getter
@Setter
@ToString
@Entity
@Table(name = "ECA_TS_HISTORY_TM")
public class ToastHistory {
    @Id
    @Column(name = "ENTITY_ID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long entityId;

    @Column(name = "SEND_TYPE")
    private String sendType;

    @Column(name = "REQUEST_ID")
    private String requestId;

    @Column(name = "SENDER")
    private String sender;

    @Column(name = "TEMPLATE_CD")
    private String templateCode;

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

    @Column(name = "WORK_TYPE")
    private String workType;
}
