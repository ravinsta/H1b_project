
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



public class que3{

public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
		
		
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException 
		{
	        	String row = value.toString();
	        	String[] tokens = row.split("\t");
	        	String casestatus = tokens[1];
	        	String socname = tokens[3];
	        	String jobtitle = tokens[4];
	        	
	        	if(casestatus.contains("CERTIFIED"))
	        	{
	        	if(jobtitle.contains("DATA SCIENTIST"))	       	
	        	{
	        	context.write(new Text(socname),new IntWritable(1));
	        	}
		}  
		}
	}
	



	
	public static class MyReducer extends Reducer<Text,IntWritable, Text, IntWritable> {
	int max=0;
	String socName="nothing";
		public void reduce(Text inpK, Iterable<IntWritable> inpV, Context c) throws IOException, InterruptedException{
		
		int count=0;
		
		IntWritable result = new IntWritable();
		for(IntWritable inp:inpV){
			 
				count+=inp.get();
		}
		result.set(count);
		  if(inpK!=new Text(""))
	             if(count>max)
	             {
	                 max= count;
	                 socName=inpK.toString();
	             }
	            
	          //context.write(key, new LongWritable(sum));
	          
	        }
	        @Override
	        protected void cleanup(Context context) throws IOException, InterruptedException {
	            if(socName!="")
	            context.write(new Text(socName), new IntWritable(max));        
   
					
					
			
		}
		
	}
																																																																																								
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	Configuration conf = new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
    job.setJarByClass(que3.class);
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

