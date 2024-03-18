package com.s3.eca2.domain.ticketChannel;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


import javax.persistence.*;
import java.util.Date;


@Getter
@Setter
@ToString
@Entity
@Table(name = "ECA_CS_TICKET_CHANNEL_TM")
public class TicketChannel {
    @Id
    @Column(name = "TICKET_CHANNEL_EID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long ticketChannelEid;

    @Column(name = "ENTITY_STATUS")
    private String entityStatus;

    @Column(name = "MOD_DATE")
    private Date modDate;

    @Column(name = "REG_DATE")
    private Date regDate;

    @Column(name = "CONTACT_CD")
    private String contactCode;

    @Column(name = "END_DATE")
    private String endDate;

    @Column(name = "START_DATE")
    private String startDate;

    @Column(name = "TICKET_EID")
    private Long ticketEid;

    @Column(name = "TYPE_CD")
    private String typeCode;

    @Column(name = "PROCESS_DATE")
    private String processDate;

    @Column(name = "MOD_USER_ENTITY_ID")
    private Long modUserEntityId;

    @Column(name = "REG_USER_ENTITY_ID")
    private Long regUserEntityId;
}
