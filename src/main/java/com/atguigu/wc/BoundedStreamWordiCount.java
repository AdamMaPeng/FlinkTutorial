package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  对于批量文件，在Flink 中可以当作为有界流进行处理
 */
public class BoundedStreamWordiCount {
    public static void main(String[] args) throws Exception {
         //1、flink 流式执行环境准备
        StreamExecutionEnvironment exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2、读取文件中的数据
        DataStreamSource<String> linesDS = exeEnv.readTextFile("input/words.txt");

        // 3、将读取到的数据进行处理
        linesDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 将每 line 数据进行切分
            String[] words = line.split(" ");

            // 将 words 中的每个word 进行处理
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1)
                .print();

        exeEnv.execute();
    }
}
