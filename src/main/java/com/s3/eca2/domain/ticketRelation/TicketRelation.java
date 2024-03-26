package com.s3.eca2.domain.ticketRelation;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;
import java.util.Date;


@Getter
@Setter
@ToString
@Entity
@Table(name = "ECA_CS_TICKET_RELATION_TM")
public class TicketRelation {
    @Id
    @Column(name = "TICKET_RELATION_EID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long ticketRelationEid;

    @Column(name = "ENTITY_STATUS")
    private String entityStatus;

    @Column(name = "MOD_DATE")
    private Date modDate;

    @Column(name = "REG_DATE")
    private Date regDate;

    @Column(name = "TARGET_TICKET_EID")
    private Long targetTicketEid;

    @Column(name = "TICKET_EID")
    private Long ticketEid;

    @Column(name = "TYPE_CD")
    private String typeCode;

    @Column(name = "MOD_USER_ENTITY_ID")
    private Long modUserEntityId;

    @Column(name = "REG_USER_ENTITY_ID")
    private Long regUserEntityId;
}
