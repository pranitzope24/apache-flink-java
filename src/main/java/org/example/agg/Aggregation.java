package org.example.agg;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.StringReader;
import java.util.*;

public class Aggregation {
    public static void main (String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);

        DataStream<String> stringData = env.readTextFile(params.get("input")); //add param -> path to rawdata.txt
        int n = params.getInt("size"); //add param -> size of list to be maintained

        DataStream<JsonObject> data = stringData
                .map(new JsonConverterMap());

        SingleOutputStreamOperator<JsonObject> objData = data
                .filter(obj -> obj.get("id").getAsString().startsWith("nbpay"))
                .filter(new StatusFilter());

        DataStream<Double> successPercentage = objData
                .flatMap(new StatusChecker(n))
                .map(new SuccessPercentageCalculator());

        successPercentage.print();
        env.execute("Success Calculator");
    }

    public static class StatusChecker implements FlatMapFunction<JsonObject, List<Integer>> {

        private List<Integer> list = new ArrayList<>();
        private int maxSize;

        public StatusChecker(int size) {
            this.maxSize = size;
        }

        @Override
        public void flatMap(JsonObject obj, Collector<List<Integer>> collector) {
            String status = obj.get("state").getAsString();

            if(list.size() >= this.maxSize) {
                list.remove(0);
            }

            list.add( status.contains("SUCCESS") ? 1 : 0 );
            collector.collect(list);
        }
    }

    public static class SuccessPercentageCalculator implements MapFunction<List<Integer>, Double> {

        @Override
        public Double map(List<Integer> list) {
            return successPercent(list);
        }
        public Double successPercent(List <Integer> list) {
            int sum =0;
            for(int x : list){
                sum += x;
            }
            return (double) (sum*100)/(double) list.size();
        }
    }

    public static class JsonConverterMap implements MapFunction<String, JsonObject>{
        private transient Gson gson;

        public JsonObject map(String s) {
            if(gson == null){
                gson = new Gson();
            }
            JsonReader jsonReader = new JsonReader(new StringReader(s));
            jsonReader.setLenient(true);

            return gson.fromJson(jsonReader, JsonObject.class);
        }

    }


    public static class StatusFilter implements FilterFunction<JsonObject> {

        @Override
        public boolean filter(JsonObject obj) {
            String s = obj.get("state").getAsString();
            return (s.contains("SUCCESS") || s.contains("FAILED"));
        }
    }

}
