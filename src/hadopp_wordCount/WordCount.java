package hadopp_wordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	//map
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer iter = new StringTokenizer(value.toString());
			while (iter.hasMoreTokens()) {
				
				word.set(iter.nextToken()); 
				context.write(word, one);
				
			}
		}
	}
	
	//reduce
	public static class reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Context cont) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : value) {
				sum += i.get();
			}
			result.set(sum);
			cont.write(key, result);
		}
	}
	
	//main
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.out.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "wordCount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(reduce.class);
		job.setReducerClass(reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		for(int i = 0; i < otherArgs.length -1; i++)
		{
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
				
	}
	
	
}
