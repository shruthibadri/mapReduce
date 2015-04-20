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

public class EastWest {

    
    public static class EastWestMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    int maxElevLat = 12345;  
            String line = value.toString();
	    int station = Integer.parseInt(line.substring(4, 9))
                      
	    int lat;		
	 	
	    if (line.charAt(28) == '+') { 
		    lat = Integer.parseInt(line.substring(29, 33));
		} else {
		    lat = Integer.parseInt(line.substring(28, 33));
	    }

            int value = (abs(elevation) * 1000000 + station) * signum(elevation);
		context.write(dummykey, new IntWritable (value));
	    
	}
    } // class MaxElevMapper

    public static class MaxElevReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			   Context context)
	    throws IOException, InterruptedException {
	    
	    int maxValue = Integer.MIN_VALUE;
            int maxStation;
	    for (IntWritable value : values) {
            int elevation = value.get()/1000000;
            int station = value.get()%1000000;     

            maxValue = Math.max(maxValue, elevation);
            if maxValue == elevation{
		maxStation = station;
             }
	    }
           
	    context.write(key, new IntWritable(maxStation));
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
