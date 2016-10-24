import org.apache.commons.configuration.SystemConfiguration;
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

import java.io.IOException;

public class FlightInfo {
    public static class FlightInfoMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text route = new Text();
        private IntWritable route_delay = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.contains("Year,Month")) {
                /**
                 * Split function too slow,
                 * manipulate char array faster
                 */
                /*
                String[] cols = line.split(",");
                int delay_num = 0;
                if (!cols[14].equals("NA")) {
                    delay_num = Integer.parseInt(cols[14]);
                }
                route.set(cols[16]+"-"+cols[17]);
                route_delay.set(delay_num);
                context.write(route, route_delay);
                */

                int[] cols_index = {14, 16, 17};
                String[] cols = getCol(line.toCharArray(), ',', cols_index);
                int delay_num = 0;
                if (!cols[0].equals("NA")) {
                    delay_num = Integer.parseInt(cols[0]);
                }
                route.set(String.format("%s-%s", cols[1], cols[2]));
                route_delay.set(delay_num);
                context.write(route, route_delay);

            }
        }

        private int[] delim_idx = new int[30];
        private String[] getCol(char[] chArray, char delim, int[] n) {
            int cur_delim = 1;
            int i = 0;
            delim_idx[0] = 0;
            while (i < chArray.length) {
                if (chArray[i] == delim) {
                    delim_idx[cur_delim++] = i;
                    if (cur_delim > n[n.length-1] +1) {
                        break;
                    }
                }
                i++;
            }
            if (i >= chArray.length && chArray[chArray.length-1] != delim) {
                delim_idx[cur_delim++] = chArray.length;
            }
            String[] result = new String[n.length];
            for (i=0; i<n.length; i++) {
                if (n[i] < cur_delim) {
                    result[i] = new String(chArray, delim_idx[n[i]]+1, delim_idx[n[i]+1] - delim_idx[n[i]] - 1);
                } else {
                    result[i] = "";
                }
            }
            return result;
        }
    }

    public static class FlightInfoReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            long sum = 0; // prevent sum overflow.
            int cnt = 0;
            for (IntWritable val : values) {
                sum += val.get();
                cnt++;
            }
            int avg = (int)(sum/cnt);
            result.set(avg);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Flight Info");
        job.setJarByClass(Flight.class);

        job.setMapperClass(FlightInfoMapper.class);
        job.setReducerClass(FlightInfoReducer.class);
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
