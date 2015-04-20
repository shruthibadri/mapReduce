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

    
    public static class EastWestMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    int compare = 12345; //longtitude of max elev station

            String line = value.toString();
	    int station = Integer.parseInt(line.substring(4, 9));
            int lon;
                   		
	    if (line.charAt(34) == '+') { 
		    lon = Integer.parseInt(line.substring(35, 40));
		} else {
		    lon = Integer.parseInt(line.substring(34, 40));
	    }

            int eastwest = 0;
            if (lon > compare){
             eastwest = 1;
            } if (lon <compare){
            eastwest = -1;

            int lat;
 	    if (line.charAt(28) == '+') { 
		    lat = Integer.parseInt(line.substring(29, 33));
		} else {
		    lat = Integer.parseInt(line.substring(28, 33));
	    }



            int value = (station + abs(lon)*10^6) * signum(lat);
		context.write(new IntWritable (eastwest), new IntWritable (value));
	    
	}
    } // class MaxElevMapper (still in progress - doing parts 2 and 3 together)

    public static class MaxElevReducer extends Reducer<Text, IntWritable, Text, Text> {
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values,
			   Context context)
	    throws IOException, InterruptedException {
	    
             int eastwest = key.get();
             if (eastwest == 1) or (eastwest == -1) {
 
	    int maxLat = Integer.MIN_VALUE;
            int maxStation;
            int total = 0;
           
           

	    for (IntWritable value : values) {
            total ++;
            if (eastwest ==1){
            int val = value.get();
            int lat = val/10^6;
            int station = abs(val%10^6);  
     
             
            maxValue = Math.max(maxLat, lat);
            if maxLat == lat{
		maxStation = station;
                
             }
	    }
           }

            String helper = "west";
            String helper2 = ''
            if (eastwest ==1){
            helper = 'east';
            helper2 = 'The northermost station of these is' + station
            }  
            
            String a = "The number of stations on the " + helper + " of the highest station is " + total + "." + helper2; 
	    context.write(key, new Text(a));
	}
       }
    } // class Max Elev Reducer


    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "max temp");
	job.setJarByClass(MaxElevclass);
	job.setMapperClass(MaxElevMapper.class);
	//job.setCombinerClass(MaxElevMapper.class);
	job.setReducerClass(MaxElevReducer.class);
	job.setNumReduceTasks(2); // Set number of reducers
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    } //main
    
} // class MaxTemp_v2
