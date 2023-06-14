package org.example.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregationDemo {
    public static void main (String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.socketTextStream("localhost",9999);

        DataStream<Tuple4<String,String,String,Integer>> mapped = text.map(new Splitter());

        mapped.keyBy(t -> t.f0).sum(3).writeAsText("/home/pranit/out1.txt");

        mapped.keyBy(t -> t.f0).min(3).writeAsText("/home/pranit/out2.txt");

        mapped.keyBy(t -> t.f0).minBy(3).writeAsText("/home/pranit/out3.txt");

        mapped.keyBy(t -> t.f0).max(3).writeAsText("/home/pranit/out4.txt");

        mapped.keyBy(t -> t.f0).maxBy(3).writeAsText("/home/pranit/out5.txt");

        env.execute("AggregationDemo");
    }


    public static class Splitter implements MapFunction<String, Tuple4<String,String,String,Integer>> {
        @Override
        public Tuple4<String, String, String, Integer> map(String s) {
            String[] w = s.split(",");
            return new Tuple4<>(w[1], w[2], w[3], Integer.parseInt(w[4]));
        }
    }
}
