/*
 * Classname - WordCount
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

public class WordCount {

public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	// The string tokenizer class allows an application to break a string into tokens.
        StringTokenizer itr = new StringTokenizer(value.toString());
        /* The hasMoreTokens() method is used to test if there are more 
           tokens available from this tokenizer's string
        */
        while (itr.hasMoreTokens()) {
        	/* The nextToken() method is used to return the next 
               token in this string tokenizer's string
            */
            word.set(itr.nextToken());
            context.write(word, one);
      }
    }
  }
 
  public static class WordCountReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
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
    // Create a new Job Configuration object and assigning a job 
    // name for identification purposes
    Job job = Job.getInstance(conf, "word count");
    // Set the Jar by finding where a given class came from
    job.setJarByClass(WordCount.class);
    // specify a mapper
    job.setMapperClass(TokenizerMapper.class);
    // specify a reducer
    //job.setCombinerClass(WordCountReducer.class);
    job.setReducerClass(WordCountReducer.class);
    // specify output types
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    // specify hdfs input and output folder
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}