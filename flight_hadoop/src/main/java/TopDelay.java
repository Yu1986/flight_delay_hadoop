import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.tools.ant.taskdefs.PathConvert;

import java.io.IOException;
import java.util.*;

public class TopDelay {

    private static class Route {
        String route;
        Integer delay;
        public Route(String r, Integer d) {
            route = r;
            delay = d;
        }
    }

    private static Comparator<Route> mapCmp = new Comparator<Route>(){
        public int compare(Route a, Route b) {
            if (a.delay.equals(b.delay)) {
                return b.route.compareTo(a.route);
            } else {
                return a.delay - b.delay;
            }
        }
    };

    public static class TopDelayMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text route = new Text();
        private IntWritable route_delay = new IntWritable();
        public static final int K = 100;
        private PriorityQueue<Route> topDelay = new PriorityQueue<Route>(100, mapCmp);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] cols = value.toString().split("\t");
            if (cols.length != 2) {
                return;
            }
            int delay = Integer.parseInt(cols[1]);
            topDelay.add(new Route(cols[0], delay));
            if (topDelay.size() > K)
                topDelay.poll();
        }

        @Override
        protected void cleanup(Context context) throws IOException,  InterruptedException {
            Iterator<Route> ite = topDelay.iterator();
            while (ite.hasNext()) {
                Route r = ite.next();
                route.set(r.route);
                route_delay.set(r.delay);
                context.write(route, route_delay);
            }
        }
    }

    public static class TopDelayReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        private Text route = new Text();
        private IntWritable route_delay = new IntWritable();
        public static final int K = 100;
        private PriorityQueue<Route> topDelay = new PriorityQueue<Route>(100, mapCmp);

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                topDelay.add(new Route(key.toString(), val.get()));
                if (topDelay.size() > K)
                    topDelay.poll();
            }
        }

        @Override
        protected void cleanup(Reducer.Context context) throws IOException,  InterruptedException {
            Route[] routeArray = new Route[topDelay.size()];
            routeArray = topDelay.toArray(routeArray);
            Arrays.sort(routeArray, Collections.reverseOrder(mapCmp));
            for (int i=0; i<routeArray.length; i++) {
                route.set(routeArray[i].route);
                route_delay.set(routeArray[i].delay);
                context.write(route, route_delay);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Delay");
        job.setJarByClass(Flight.class);

        job.setMapperClass(TopDelayMapper.class);
        job.setCombinerClass(TopDelayReducer.class);
        job.setReducerClass(TopDelayReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        Path outPath = new Path(args[1]);
        FileSystem.getLocal(conf).delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);
    }
}
