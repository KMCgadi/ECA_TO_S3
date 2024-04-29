package com.s3.eca2.domain.counselType;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;
import java.util.Date;

@Getter
@Setter
@ToString
@Entity
@Table(name = "ECA_CS_TYPE_TM")
public class CounselType {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "COUNSEL_TYPE_EID")
    private long counselTypeEid;

    @Column(name = "ENTITY_STATUS", length = 16)
    private String entityStatus;

    @Column(name = "MOD_DATE")
    private Date modDate;

    @Column(name = "REG_DATE")
    private Date regDate;

    @Column(name = "DEPTH")
    private long depth;

    @Column(name = "NAME", length = 100)
    private String name;

    @Column(name = "ORDER_NUM")
    private long orderNum;

    @Column(name = "CODE", length = 20)
    private String code;

    @Column(name = "TEMPLATE_CODE", length = 20)
    private String templateCode;

    @Column(name = "PLUS_FRIEND_ID", length = 30)
    private String plusFriendId;

    @Column(name = "PARENT_CODE", length = 20)
    private String parentCode;

    @Column(name = "USABLE", length = 10)
    private String usable;

    @Column(name = "MOD_USER_ENTITY_ID")
    private Long modUserEntityId;

    @Column(name = "REG_USER_ENTITY_ID")
    private Long regUserEntityId;

    @Column(name = "CONTACT_TYPES", length = 50)
    private String contactTypes;

    @Column(name = "TICKET_TYPES", length = 50)
    private String ticketTypes;

    @Column(name = "DESCRIPTION", length = 1000)
    private String description;
}
