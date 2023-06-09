package org.example.wordcount;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountStreaming {
    public static void main (String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        DataStream<String> text = env.socketTextStream("localhost",9999);
        DataStream<Tuple2<String,Integer>> counts = text
                .filter(s->s.startsWith("K"))
                .map(new WordCount.Tokenizer())
                .keyBy(tuple2->tuple2.f0)
                .sum(1)
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        counts.print();
        env.execute("JOB");
    }
}


