package org.example.agg;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Aggregation {
    public static void main (String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);

        DataStream<String> data = env.socketTextStream("localhost", 9999);

        DataStream<Double> successPercentage = data
                .flatMap(new StatusChecker())
                .map(new SuccessPercentageCalculator());

        successPercentage.print();
        env.execute("Success Calculator");
    }

    public static class StatusChecker implements FlatMapFunction<String, Integer> {
        @Override
        public void flatMap(String value, Collector<Integer> out) throws Exception {
            if (value.equalsIgnoreCase("Success")) {
                out.collect(1);
            } else if (value.equalsIgnoreCase("Failure")) {
                out.collect(0);
            }
        }
    }

    public static class SuccessPercentageCalculator implements MapFunction<Integer, Double> {
        private int successCount = 0;
        private int totalCount = 0;

        @Override
        public Double map(Integer value) throws Exception {
            totalCount ++;
            successCount+= value;
            return (double) (successCount * 100) / totalCount;
        }
    }
}
