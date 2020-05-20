
import org.apache.flink.api.common.functions.FlatMapFunction;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class WordCountJava {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "/Users/loay/idea-workspace/sparktrain/FlinkTest/src/main/resources/hello.txt";

        DataStream<String> stringDataStream = executionEnvironment.readTextFile(inputPath);


         stringDataStream.flatMap(new FlatMapFunction<String, Tuple2<String , Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word: s.split(" ")) {
                    collector.collect(new Tuple2<>(word, 1));
                }

            }
        }) ;

    }

}
