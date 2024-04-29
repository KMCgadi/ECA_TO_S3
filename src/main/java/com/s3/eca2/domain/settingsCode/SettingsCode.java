package com.s3.eca2.domain.settingsCode;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;
import java.util.Date;

@Getter
@Setter
@ToString
@Entity
@Table(name = "PRF_SETTINGS_CODE")
public class SettingsCode {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ENTITY_ID")
    private long entityId;

    @Column(name = "ENTITY_STATUS", length = 16)
    private String entityStatus;

    @Column(name = "MOD_DATE")
    private Date modDate;

    @Column(name = "REG_DATE")
    private Date regDate;

    @Column(name = "PATH")
    private String path;

    @Column(name = "CODE", nullable = false)
    private String code;

    @Column(name = "NAME", length = 256, nullable = false)
    private String name;

    @Column(name = "ORDER_NUM", nullable = false)
    private int orderNum;

    @Column(name = "REMARK_TEXT", length = 1024)
    private String remarkText;

    @Column(name = "USE_TYPE_CODE", length = 10, nullable = false)
    private String useTypeCode;

    @Column(name = "MOD_USER_ENTITY_ID")
    private Long modUserEntityId;

    @Column(name = "REG_USER_ENTITY_ID")
    private Long regUserEntityId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "PARENT_EID", referencedColumnName = "ENTITY_ID")
    private SettingsCode parentCode;
}
