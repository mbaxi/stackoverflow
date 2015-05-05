package com.mbaxi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Hello world!
 *
 */
public class MapRedA 
{
	private static class MapRed_A_Mapper extends Mapper<Object,Text,Text,IntWritable> {
		private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private MultipleOutputs<Text, IntWritable> out;
	    
	    @Override
		public void setup(Context context) {
	    	out = new MultipleOutputs<Text, IntWritable>(context);
	    	
	    }
	    @Override
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	    	StringTokenizer itr = new StringTokenizer(value.toString());
	    	while (itr.hasMoreTokens()) {
	    		word.set(itr.nextToken());
	    		context.write(word, one);
	    		out.write(new Text(word), one,"MapRed_A_MapperOutput/part");
	    		//some edit
	    	}
	    }
	    @Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
	    	out.close();
	    }
	}
	private static class MapRed_A_Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	}

	public static Job getJob(String[] args) throws IOException
	{
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(MapRedA.class);
	    job.setMapperClass(MapRed_A_Mapper.class);
	    job.setReducerClass(MapRed_A_Reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job;
	}

    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
		String inputPath = args[0];
		String outPath = args[1];
		
		String[] inPaths = new String[]{inputPath, outPath}; 
				
		Job mapRedA = MapRedA.getJob(inPaths); 
		mapRedA.submit();
		mapRedA.waitForCompletion(true);
    }
}
