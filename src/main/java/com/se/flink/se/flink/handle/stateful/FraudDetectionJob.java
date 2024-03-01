package com.se.flink.se.flink.handle.stateful;

import com.se.flink.se.flink.handle.stateful.entity.Alert;
import com.se.flink.se.flink.handle.stateful.entity.Transaction;
import com.se.flink.se.flink.handle.stateful.processor.FraudDetector;
import com.se.flink.se.flink.handle.stateful.sink.AlertSink;
import com.se.flink.se.flink.handle.stateful.source.TransactionSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");
        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");
        alerts
                .addSink(new AlertSink())
                .name("send-alerts");

        env.execute("Fraud Detection");
    }
}
