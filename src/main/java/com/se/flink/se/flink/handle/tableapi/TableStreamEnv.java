package com.se.flink.se.flink.handle.tableapi;

import com.se.flink.se.flink.domain.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableStreamEnv {

    public static void main(String[] args) throws Exception {

        tableStreamAPI();
    }

    public static void tableStreamAPI() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String bootstrapServers = "localhost:9092, localhost:9093, localhost:9094";
        KafkaSource<Transaction> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("txn-flink")
                .setGroupId("table-flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema(Transaction.class))
                .build();
        DataStreamSource<Transaction> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka");
        Table value = tableEnv.fromDataStream(dataStream,
                        $("transId"), $("userId"), $("amount"), $("transTime"),
                        $("bank"), $("status"), $("totalAmount"))
                .as("TransId", "UserId", "Amount", "TransTime", "Bank", "Status", "TotalAmount");
        tableEnv.createTemporaryView("Transaction", value);
//        value.execute().print();
        tableEnv.executeSql("SELECT UserId, Sum(Amount) As AmountTT " +
                "FROM Transaction " +
                "WHERE UserId > '5' " +
                "GROUP BY UserId").print();

// add a printing sink and execute in DataStream API
    }
}
