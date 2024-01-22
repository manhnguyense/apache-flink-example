package com.se.flink.se.flink.domain;


import java.io.Serializable;

public class Transaction implements Serializable {
    private long transId;
    private String userId;
    private Long amount;
    private String transTime;
    private String bank;
    private String status;
    private Long totalAmount;

    public void setTotalAmount(Long totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Long getTotalAmount() {
        return totalAmount;
    }

    public long getTransId() {
        return transId;
    }

    public void setTransId(long transId) {
        this.transId = transId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public String getTransTime() {
        return transTime;
    }

    public void setTransTime(String transTime) {
        this.transTime = transTime;
    }

    public String getBank() {
        return bank;
    }

    public void setBank(String bank) {
        this.bank = bank;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Transaction merge(Transaction that) {

        assert (this.getUserId().equalsIgnoreCase(that.getUserId()));
        final Transaction txn = new Transaction();
        txn.setUserId(that.getUserId());
        txn.setAmount(that.getAmount());
        txn.setBank(that.getBank());
        txn.totalAmount = that.amount + this.amount;
        return txn;
    }
}
