package com.s3.eca2.domain.organizationType;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;
import java.util.Date;


@Getter
@Setter
@ToString
@Entity
@Table(name = "GAEA_ORGANIZATION_TYPE_TM")
public class OrganizationType {
    @Id
    @Column(name = "ORGANIZATION_TYPE_EID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long organizationTypeEid;

    @Column(name = "ENTITY_STATUS")
    private String entityStatus;

    @Column(name = "MOD_DATE")
    private Date modDate;

    @Column(name = "REG_DATE")
    private Date regDate;

    @Column(name = "CODE")
    private String code;

    @Column(name = "PARENT_CODE")
    private String parentCode;

    @Column(name = "DEPTH")
    private Long depth;

    @Column(name = "name")
    private String name;

    @Column(name = "ORDER_NUM")
    private Long orderNum;

    @Column(name = "MOD_USER_ENTITY_ID")
    private Long modUserEntityId;

    @Column(name = "REG_USER_ENTITY_ID")
    private Long regUserEntityId;

    @Column(name = "COMPANYCODE")
    private Long companyCode;
}
