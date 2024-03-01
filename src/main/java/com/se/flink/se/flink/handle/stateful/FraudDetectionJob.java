package com.se.flink.se.flink.handle.stateful;

import com.se.flink.se.flink.handle.stateful.entity.Alert;
import com.se.flink.se.flink.handle.stateful.entity.Transaction;
import com.se.flink.se.flink.handle.stateful.processor.FraudDetector;
import com.se.flink.se.flink.handle.stateful.sink.AlertSink;
import com.se.flink.se.flink.handle.stateful.source.TransactionSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .createRemoteEnvironment("localhost", 8081, "target/flink-demon-v1.0.0.jar");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofSeconds(5));
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // delay
        ));
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
