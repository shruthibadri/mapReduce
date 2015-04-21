import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class MaxElev {

    public static class MaxElevMapper
        extends Mapper<LongWritable, Text, Text, ArrayWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String line = value.toString();

            int station = Integer.parseInt(line.substring(4, 10));
            int elev = Integer.parseInt(line.substring(47, 51));
            int longitude = Integer.parseInt(line.substring(34, 41));

            Text dummykey = new Text();
            ArrayWritable value =
                new ArrayWritable(IntWritable,
                                  {new IntWritable(station),
                                   new IntWritable(elev),
                                   new IntWritable(longitude)});

            context.write(dummykey, value);
        }
    } // class MaxElevMapper

    public static class MaxElevReducer
        extends Reducer<Text, ArrayWritable, Text, ArrayWritable> {
        @Override
        public void reduce(Text key, Iterable<ArrayWritable> values,
                           Context context)
            throws IOException, InterruptedException {

            int maxElev = Integer.MIN_VALUE;
            int maxStation;
            int maxLongitude;
            for (ArrayWritable value : values) {
                Writable[] array = value.get();
                int station = array[0].get();
                int elev = array[1].get();
                int longitude = array[2].get();

                if (elev > maxElev) {
                    maxElev = elev;
                    maxStation = station;
                    maxLongitude = longitude;
                }
            }

            ArrayWritable value =
                new ArrayWritable(
                                  IntWritable,
                                  {new IntWritable(maxStation),
                                   new IntWritable(maxElev)});
            context.write(key, value);
        }
    } // class Max Elev Reducer

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max temp");
        job.setJarByClass(MaxElevclass);
        job.setMapperClass(MaxElevMapper.class);
        //job.setCombinerClass(MaxElevMapper.class);
        job.setReducerClass(MaxElevReducer.class);
        job.setNumReduceTasks(1); // Set number of reducers
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } //main

} // class MaxTemp_v2
