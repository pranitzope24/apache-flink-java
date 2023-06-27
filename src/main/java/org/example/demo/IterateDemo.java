package org.example.demo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterateDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Long, Integer>> data = env.fromSequence(0, 5)
                        .map((MapFunction<Long, Tuple2<Long, Integer>>) value -> new Tuple2<>(value, 0));

        IterativeStream<Tuple2<Long, Integer>> iteration = data.iterate(5000);
        DataStream<Tuple2<Long, Integer>> plusOne = iteration
                .map(new MapFunction<Tuple2<Long, Integer > , Tuple2<Long, Integer>> () {
                    public Tuple2<Long, Integer > map(Tuple2<Long, Integer > value) {
                        return value.f0 == 10 ? value : new Tuple2<Long, Integer>(value.f0 + 1, value.f1 + 1);
                    }
                });

        DataStream<Tuple2<Long, Integer>> notEqualToTen = plusOne
                .filter(new FilterFunction<Tuple2<Long, Integer>> () {
                    public boolean filter(Tuple2<Long, Integer > value) {
                        return value.f0 != 10;
                    }
                });
        iteration.closeWith(notEqualToTen);

        DataStream<Tuple2<Long, Integer>> equalToTen =
                plusOne.filter(new FilterFunction<Tuple2<Long, Integer>> () {
                    public boolean filter(Tuple2<Long, Integer > value) {
                        return value.f0 == 10;
                    }
                });

        equalToTen.print();

        env.execute("Iteration Demo");
    }
}