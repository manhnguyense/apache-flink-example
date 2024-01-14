package com.se.flink.se.flink.domain;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;

@JsonPropertyOrder({"cardMasking", "tokenNo", "phone", "holderName", "f6no", "l4no", "clientUserId"})
public class CardVM implements Serializable {
    private String cardMasking;
    private String tokenNo;
    private String phone;
    private String holderName;
    private String cardNumber;
    private String f6no;
    private String l4no;
    private String clientUserId;

    public String getClientUserId() {
        return clientUserId;
    }

    public void setClientUserId(String clientUserId) {
        this.clientUserId = clientUserId;
    }

    public String getF6no() {
        return f6no;
    }

    public void setF6no(String f6no) {
        this.f6no = f6no;
    }

    public String getL4no() {
        return l4no;
    }

    public void setL4no(String l4no) {
        this.l4no = l4no;
    }

    public String getTokenNo() {
        return tokenNo;
    }

    public void setTokenNo(String tokenNo) {
        this.tokenNo = tokenNo;
    }

    public String getHolderName() {
        return holderName;
    }

    public void setHolderName(String holderName) {
        this.holderName = holderName;
    }

    public String getCardNumber() {
        return cardNumber;
    }

    public void setCardNumber(String cardNumber) {
        this.cardNumber = cardNumber;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getCardMasking() {
        return cardMasking;
    }

    public void setCardMasking(String cardMasking) {
        this.cardMasking = cardMasking;
    }


    @Override
    public String toString() {
        return StringUtils.isNotEmpty(cardNumber) ?
                cardMasking + "," +
                        tokenNo + "," +
                        phone + "," +
                        holderName + "," +
                        f6no + "," +
                        l4no + "," +
                        clientUserId + "," +
                        cardNumber :
                cardMasking + "," +
                        tokenNo + "," +
                        phone + "," +
                        holderName + "," +
                        f6no + "," +
                        l4no + "," +
                        clientUserId;
    }
}
