package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  使用 Flink 处理 流式数据
 *      1、与 批处理方式 DataSet 不同的是，Flink进行流式计算的时候，执行环境也不同：StreamExecutionEnv
 *
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1、流式环境准备
        StreamExecutionEnvironment streamExeEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2、从一个源头不断的数据，此处采用 socket 的方式，源头来自于 NetCat 工具获取到的某个服务器来的数据
        DataStreamSource<String> lineStreamSource = streamExeEnv.socketTextStream("hadoop102", 7777);

        // 3、对读取到的每行的数据进行处理
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 将每 line 的数据进行切分
            String[] words = line.split(" ");
            // 将 words 中的每个 word 处理为 （word,1）
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4、将处理完毕的 (word,1) 进行聚合处理
//        KeyedStream<Tuple2<String, Long>, Tuple> tuple2TupleKeyedStream = wordAndOneTuple.keyBy(0);

        KeyedStream<Tuple2<String, Long>, String> tuple2TupleKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2TupleKeyedStream.sum(1);

        sum.print();

        streamExeEnv.execute();
    }
}
