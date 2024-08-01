package com.s3.eca2.domain.ticketOrder;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.*;
import java.util.Date;


@Getter
@Setter
@ToString
@Entity
@Table(name = "ECA_CS_TICKET_ORDER_TM")
public class TicketOrder {
    @Id
    @Column(name = "TICKET_ORDER_EID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long ticketOrderEid;

    @Column(name = "TICKET_EID")
    private Long ticketEid;

    @Column(name = "ORDER_ID")
    private String orderId;

    @Column(name = "PRODUCT_ID")
    private String productId;

    @Column(name = "PRODUCT_NAME")
    private String productName;

    @Column(name = "COUPON_ID")
    private String couponId;

    @Column(name = "PARTNER_NAME")
    private String partnerName;

    @Column(name = "BRAND_NAME")
    private String brandName;

    @Column(name = "CATEGORY")
    private String cateGory;

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

    @Column(name = "DELIVERY_METHOD")
    private String deliveryMethod;

    @Column(name = "PAYMENT_METHOD")
    private String paymentMethod;

    @Column(name = "RATING")
    private String rating;

    @Column(name = "ORDER_OPTION_ID")
    private String orderOptionId;
}
