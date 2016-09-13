/*
 * Classname - CommentWordCount
 *
 * Version - V1.0
 *
 * Copyright - Data Application Lab
 */

package com.neutronmobile.word_count;

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

public class CommentWordCount {

	public static class WordCountMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Grab the "Text" field, since that is what we are counting over
			String txt = value.toString();

			// Will return null if the key is not there
			if (txt == null) {
				// skip this record
				return;
			}

			// Convert to small case
			txt = txt.toLowerCase();

			// Remove some annoying punctuation
			txt = txt.replaceAll("'", ""); // remove single quotes
			txt = txt.replaceAll("[^a-zA-Z]", " "); // replace the rest with a space

			// Tokenize the string, then send the tokens to reducer
			StringTokenizer itr = new StringTokenizer(txt);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			result.set(sum);
			context.write(key, result);

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(CommentWordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	    // specify HDFS input and output folder
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
