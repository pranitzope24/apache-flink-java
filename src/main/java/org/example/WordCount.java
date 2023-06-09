package org.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<String> filtered = text.filter((FilterFunction<String>) s -> s.startsWith("N"));

        DataSet<Tuple2<String,Integer>> tokenized = filtered.map(new Tokenizer());
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);

        if (params.has("output")){
            counts.writeAsCsv(params.get("output"), "\n"," ");
            env.execute("WordCount Example");
        }
    }

    public static final class Tokenizer implements MapFunction<String,Tuple2<String,Integer>>{

        @Override
        public Tuple2<String, Integer> map(String s) {
            return new Tuple2<>(s, 1);
        }
    }
}