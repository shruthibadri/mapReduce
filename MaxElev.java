import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class MaxElev {


    public static class MaxElevMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    int station = Integer.parseInt(line.substring(4, 9));
            private Text dummykey = new Text();
	    int lon;
            int elev = Integer.parseInt(line.substring(48, 51));

	    if (line.charAt(34) == '+') {
		    lon = Integer.parseInt(line.substring(35, 40));
		} else {
		    lon = Integer.parseInt(line.substring(34, 40));
	    }

            int value = (elev + station*10^4 + abs(lon)*10^10) * signum(lon);
		context.write(dummykey, new IntWritable (value));

	}
    } // class MaxElevMapper

    public static class MaxElevReducer extends Reducer<Text, IntWritable, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			   Context context)
	    throws IOException, InterruptedException {

	    int maxValue = Integer.MIN_VALUE;
            int maxStation;
            int maxLon;
	    for (IntWritable value : values) {
            int val = value.get();
            int lon = val/10^10;
            int station = abs(val%10^10)/10^4;
            int elevation = val%10^4;

            maxValue = Math.max(maxValue, elevation);
            if maxValue == elevation{
		maxStation = station;
                maxLon = lon;
             }
	    }

            String a = "The station number is " + maxStation + ", which is at longtitude "  + maxLon;
	    context.write(key, new Text(a));
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
