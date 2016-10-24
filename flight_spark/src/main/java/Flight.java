
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class Flight {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: java -jar flight.jar input output");
            return;
        }

        SparkConf conf = new SparkConf().setAppName("Flight Delay Stats").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(args[0]);
        JavaPairRDD<String, Integer> route_avg = textFile.mapToPair((line)->{
            if (!line.contains("Year,Month")) {
                String[] cols = line.split(",");
                int delay_num = 0;
                if (!cols[14].equals("NA")) {
                    delay_num = Integer.parseInt(cols[14]);
                }
                String route = String.format("%s-%s", cols[16], cols[17]);
                return new Tuple2<String, Tuple2<Long, Integer>>(route, new Tuple2<Long, Integer>((long)delay_num, 1));
            } else {
                return new Tuple2<String, Tuple2<Long, Integer>>("", new Tuple2<Long, Integer>((long)0, 1));
            }
        }).reduceByKey((a,b)->{
            return new Tuple2<Long, Integer>(a._1()+b._1(), a._2()+b._2());
        }).mapToPair((t)->{
            return new Tuple2<String, Integer>(t._1(), (int)(t._2()._1()/t._2()._2()));
        });

        List<Tuple2<String, Integer>> top = route_avg.top(100, new Comp());

        PrintWriter writer = new PrintWriter(args[1]+"/out.txt", "UTF-8");
        for (Tuple2<String, Integer> t : top) {
            writer.println(String.format("%s\t%s", t._1().toString(), t._2().toString()));
        }
        writer.close();

        /*
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(args[1]), true);
        route_avg.saveAsTextFile(args[1]);
        */
    }

    private static class Comp implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
            if (a._2().equals(b._2())) {
                return b._1().compareTo(a._1());
            } else {
                return a._2() - b._2();
            }
        }
    }
}

