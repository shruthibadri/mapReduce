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
import java.util.HashSet;

public class EastWest {

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

    public static class EastWestMapper
        extends Mapper<LongWritable, Text, IntWritable, IntArrayWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            // XXX longtitude of highest station; this needs to be updated
            int compare = 12345;
            String line = value.toString();

            int station = Integer.parseInt(line.substring(4, 10));
            int longitude = line.charAt(34) == '+' ?
                Integer.parseInt(line.substring(35, 41)) :
                Integer.parseInt(line.substring(34, 41));

            if (longitude != compare) {
                int intkey = longitude > compare ? 1 : -1;
                int latitude = line.charAt(28) == '+' ?
                    Integer.parseInt(line.substring(29, 34)) :
                    Integer.parseInt(line.substring(28, 34));

                context.write(new IntWritable(intkey),
                              new IntArrayWritable(new int[]{station, latitude}));
            }
        }
    }

    public static class EastWestReducer
        extends Reducer<IntWritable, IntArrayWritable, IntWritable, IntWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<IntArrayWritable> values,
                           Context context)
            throws IOException, InterruptedException {

            int intkey = key.get();
            int maxLat = Integer.MIN_VALUE;
            int maxStation = 0;
            HashSet<Integer> seen = new HashSet<Integer>();

            for (IntArrayWritable value : values) {
                int station = array[0].get();
                int latitude = array[1].get();
                seen.add(station);

                // If station is on east and more north
                if ((intkey == 1) && (latitude > maxLat)) {
                    maxStation = station;
                    maxLat = latitude;
                }
            }

            // Write how many seen on each side
            context.write(key, new IntWritable(seen.size()));

            // Write northenmost station info
            if (key == 1) {
                context.write(new IntWritable(2), new IntWritable(maxStation));
                context.write(new IntWritable(3), new IntWritable(maxLat));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max temp");
        job.setJarByClass(EastWest.class);
        job.setMapperClass(EastWestMapper.class);
        job.setReducerClass(EastWestReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

} // class MaxTemp_v2
