package org.example.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfit {
    public static void main(String[] args) throws Exception{
        System.out.println("\n\n\n\n");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> data = env.socketTextStream("localhost",9999);

        DataStream<Tuple5<String,String,String,Integer,Integer>> mapped = data
                .map(new Splitter());

        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped
                .keyBy(t->t.f1)
                .reduce(new Reducer());

        DataStream<Tuple2<String,Double>> profitPM = reduced
                .map(new ProfitCalculator());

        profitPM.print();
        env.execute("avg profit");
    }

    public static class ProfitCalculator implements MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>>{
        @Override
        public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> in) throws Exception {
            return new Tuple2<>(in.f0, new Double((in.f3 * 1.0) / in.f4));
        }
    }

    public static class Reducer implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {

        @Override
        public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current,
                                                                       Tuple5<String, String, String, Integer, Integer> pre_res) throws Exception {
            return new Tuple5<>(
                    current.f0,
                    current.f1,
                    current.f2,
                    current.f3+pre_res.f3,
                    current.f4+pre_res.f4
            );
        }
    }

    public static class Splitter implements MapFunction<String, Tuple5<String,String,String,Integer,Integer>> {
        @Override
        public Tuple5<String, String, String, Integer, Integer> map(String s) throws Exception {
            String[] w = s.split(",");
            return new Tuple5<>(w[1], w[2], w[3], Integer.parseInt(w[4]), 1);
        }
    }
}
