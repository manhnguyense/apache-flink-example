package com.se.flink.se.flink.handle.stream;

import com.google.gson.Gson;
import com.se.flink.se.flink.domain.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkKafkaDataStream {

    public static void main(String[] args) throws Exception {
        handleTxn();
    }

    public static void handleTxn() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        String bootstrapServers = "localhost:9092, localhost:9093, localhost:9094";
        KafkaSource<Transaction> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("txn-flink-prod")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema(Transaction.class))
                .build();

        DataStreamSource<Transaction> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka");
        DataStream<Transaction> map = dataStream
                .map((MapFunction<Transaction, Transaction>) transaction -> {
                    transaction.setStatus("SUC");
                    return transaction;
                });

        DataStream<Transaction> filter = map.filter((FilterFunction<Transaction>)
                        transaction -> transaction.getAmount() > 3000)
                .keyBy(Transaction::getUserId);
        KafkaSink<Transaction> sink = KafkaSink.<Transaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("txn-flink-cons").setValueSerializationSchema(
                                (SerializationSchema<Transaction>) transaction ->
                                        new Gson().toJson(transaction).getBytes()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        filter.print();
        filter.sinkTo(sink);
        env.execute();
    }
}
