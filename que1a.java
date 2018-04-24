
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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class que1a{

public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
		LongWritable one = new LongWritable(1);
		
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException 
		{
			if(key.get()>0)
        	{
        	
	        	String row = value.toString();
	        	String[] tokens = row.split("\t");
	           	String jobtitle = tokens[4];
	        	String year = tokens[7];
	        	
	    
	        	if(tokens[4]!=null && tokens[4].equalsIgnoreCase("DATA ENGINEER"))	       	
	        	{
	        		Text fin = new Text(jobtitle+","+year);
	        	context.write(fin,one);
	        	}
        	}
		}
	}
	



	
	public static class MyReducer extends Reducer<Text,LongWritable, Text, LongWritable> {
		LongWritable SUM=new LongWritable(0); int i=0; 
        String[] years={"2011","2012","2013","2014","2015","2016"}; 
long [] arr=new long[6]; 
public void reduce(Text key,Iterable<LongWritable> values ,Context context) throws IOException, InterruptedException
{ 
    long sum=0; 
    for(LongWritable val:values) 
    sum+=val.get(); 
    arr[i++]=sum; 
} 
 
public void cleanup(Context context) throws IOException, InterruptedException 
{ 
for (int i=0;i<6;i++)     
if (i==0) 
context.write(new Text(years[i]), new LongWritable(0)); 
else 
    context.write(new Text(years[i]),new LongWritable((arr[i]-arr[i-1])*100/arr[i-1])); 

} 
		
	}
	
    																																																																																					
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	Configuration conf = new Configuration();
	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
    job.setJarByClass(que1a.class);
    job.setJobName("task 3");
    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

