package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1、准备批处理的 flink 环境
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        // 2、通过批处理的环境读取文件
        DataSource<String> stringDS = batchEnv.readTextFile("input/words.txt");

        // 3、将读取到的每行的数据进行处理，处理成（word，1）进行计算
        FlatMapOperator<String, Tuple2<String, Long>> wordToOneTuple = stringDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 将 每行数据处理成 一个个的单词
            String[] words = line.split(" ");
            // 遍历 words 这个集合，将每个单词处理成 (word,1)
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4、根据 key 进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordToOneTuple.groupBy(0);

        // 5、将分组后的数据进行聚合
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        // 6、 打印结果
        sum.print();
    }
}
