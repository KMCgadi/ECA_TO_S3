package com.s3.eca2.domain.attachUrl;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


import javax.persistence.*;
import java.util.Date;

@Getter
@Setter
@ToString
@Entity
@Table(name = "ECA_CT_ATTACH_URL_TM")
public class AttachUrl {
    @Id
    @Column(name = "ATTACH_URL_EID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long attachUrlEid;

    @Column(name = "LINK_TYPE")
    private String linkType;

    @Column(name = "TICKET_ID")
    private Long ticketId;

    @Column(name = "SHORT_URL")
    private String shortUrl;

    @Column(name = "SEND_DATE")
    private Date sendDate;

    @Column(name = "SUBMIT_DATE")
    private Date submitDate;

    @Column(name = "LINK_STATUS")
    private String linkStatus;

    @Column(name = "ENTITY_STATUS")
    private String entityStatus;

    @Column(name = "REG_DATE")
    private Date regDate;

    @Column(name = "REG_USER_ENTITY_ID")
    private String regUserEntityId;

    @Column(name = "MOD_DATE")
    private Date modDate;

    @Column(name = "MOD_USER_ENTITY_ID")
    private String modUserEntityId;
}
