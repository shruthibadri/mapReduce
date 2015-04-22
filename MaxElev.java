import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class MaxElev {

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(int[] ints) {
            super(IntWritable.class);
            IntWritable[] values = new IntWritable[ints.length];
            for (int i = 0; i < ints.length; i++) {
                values[i] = new IntWritable(ints[i]);
            }
            set(values);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("[ ");

            for (String s : super.toStrings()) {
                sb.append(s).append(" ");
            }

            sb.append(" ]");
            return sb.toString();
        }
    }

    public static class MaxElevMapper
        extends Mapper<LongWritable, Text, Text, IntArrayWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String line = value.toString();

            int station = Integer.parseInt(line.substring(4, 10));
            int elev = Integer.parseInt(line.substring(46, 51));
            int longitude;
            if (line.charAt(34) == '+') {
                longitude = Integer.parseInt(line.substring(35, 41));
            }
            else {
                longitude = Integer.parseInt(line.substring(34, 41));
            }

            Text dummykey = new Text();
            IntArrayWritable out_value =
                new IntArrayWritable(new int[]{station, elev, longitude});

            context.write(dummykey, out_value);
        }
    }

    public static class MaxElevReducer
        extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {
        @Override
        public void reduce(Text key, Iterable<IntArrayWritable> values,
                           Context context)
            throws IOException, InterruptedException {

            int maxElev = Integer.MIN_VALUE;
            int maxStation = 0;
            int maxLongitude = 0;
            for (IntArrayWritable value : values) {
                IntWritable[] array = (IntWritable[])value.toArray();
                int station = array[0].get();
                int elev = array[1].get();
                int longitude = array[2].get();

                if (elev > maxElev) {
                    maxElev = elev;
                    maxStation = station;
                    maxLongitude = longitude;
                }
            }

            IntArrayWritable value =
                new IntArrayWritable(new int[]{maxStation, maxElev});
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max elev");
        job.setJarByClass(MaxElev.class);
        job.setMapperClass(MaxElevMapper.class);
        job.setReducerClass(MaxElevReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntArrayWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
