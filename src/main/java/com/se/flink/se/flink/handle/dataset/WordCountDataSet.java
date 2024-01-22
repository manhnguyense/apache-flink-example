package com.se.flink.se.flink.handle.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountDataSet {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 8081, "target/flink-demon-v1.0.0.jar");
        DataSet<String> text = env.fromElements(
                "The old oak tree stood stoic in the field, its branches reaching " +
                        "towards the sky like gnarled fingers. Beneath its shade," +
                        " a young girl sat with a book in her lap, lost in the world of stories. " +
                        "The wind rustled through the leaves, whispering secrets only " +
                        "the ancient tree could understand. In the distance, a church bell chimed," +
                        " marking the passage of time. A gentle breeze carried the scent of wildflowers, " +
                        "painting the air with a sweet fragrance. The girl turned a page," +
                        " her eyes sparkling with wonder as she continued her journey through the pages.");

        DataSet<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        wordCounts.print();
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
