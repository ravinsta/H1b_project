

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class que7{

public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
		
		
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException 
		{
	        	String row = value.toString();
	        	String[] tokens = row.split("\t");  	
	   	        context.write(new Text(tokens[7]),new IntWritable(1));
	        	
		
		}
	}
	

	
	public static class MyReducer extends Reducer<Text,IntWritable, Text, IntWritable> {
	
		protected void reduce(Text inpK, Iterable<IntWritable> inpV, Context context) throws IOException, InterruptedException{
		
		int count=0;
		
		IntWritable result = new IntWritable();
		for(IntWritable inp:inpV){
			 
				count+=inp.get();
		}
		result.set(count);
		context.write(inpK,result);
	        
	        }
		
	       
		
	}
																																																																																								
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	Configuration conf = new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
    job.setJarByClass(que7.class);
    job.setJobName("task 3");
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


