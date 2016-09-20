package com.example.dummy;


import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import com.google.common.collect.TreeMultimap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;

public class FlinkMultimapDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        /*
        Sets up custom Kryo serializer by one of the following three ways:

        env.addDefaultKryoSerializer(TreeMultimap.class, new TreeMultimapSerializer<>(String.class));
        env.addDefaultKryoSerializer(TreeMultimap.class, new GenericJavaSerializer<TreeMultimap>());
        env.getConfig().addDefaultKryoSerializer(TreeMultimap.class, new GenericJavaSerializer<TreeMultimap>());
        */

        env.addDefaultKryoSerializer(TreeMultimap.class, new TreeMultimapSerializer<>(String.class));

        env.setParallelism(2);

        String words = "Alpha Bravo Charlie Delta Echo Foxtrot Golf Hotel India Juliet Kilo Lima " +
                "Mike November Oscar Papa Quebec Romeo Sierra Tango Uniform Victor Whiskey X-ray Yankee Zulu";

        DataStream<String> wordStream = env.fromCollection(Arrays.asList(words.split("\\s+")));
        KeyedStream<Tuple2<String, Integer>, Integer> keyedStream =
                wordStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        // Coincidentally, the count of each item is exactly the length of its name
                        return Tuple2.of(s, s.length());
                    }
                }).keyBy(new KeySelector<Tuple2<String, Integer>, Integer>() {
                    public Integer getKey(Tuple2<String, Integer> x) throws Exception {
                        // return x.f0.hashCode();
                        return 0;
                    }
                });

        // `topKSoFar.put` throws a 'java.lang.NullPointerException' because the underlying map is null.
        // However, if you remove the `timeWindow(...)` call no error will occur.
        keyedStream.timeWindow(Time.minutes(1)).fold(TreeMultimap.<Integer, String>create(),
                new FoldFunction<Tuple2<String, Integer>, TreeMultimap<Integer, String>>() {
                    public TreeMultimap<Integer, String> fold(TreeMultimap<Integer, String> topKSoFar,
                                                              Tuple2<String, Integer> itemCount) throws Exception {
                        String item = itemCount.f0;
                        Integer count = itemCount.f1;
                        topKSoFar.put(count, item);
                        if (topKSoFar.keySet().size() > 3) {
                            topKSoFar.removeAll(topKSoFar.keySet().first());
                        }
                        return topKSoFar;
                    }
                }).print();

        env.execute();
    }
}
