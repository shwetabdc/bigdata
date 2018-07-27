
package com.cloudxlab.titanic;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudxlab.customreader.NLinesInputFormat;

public class Female_survived {


public static void main(String[] args) throws Exception {
 
//Configuration conf=new Configuration();
//Job job=new Job();

JobConf conf = new JobConf();
Job job = new Job(conf, "titanicjob"); 
 
 
job.setJarByClass(Female_survived.class);
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setOutputKeyClass(Key_value.class);
job.setMapOutputKeyClass(Key_value.class);
job.setMapOutputValueClass(IntWritable.class);
job.setOutputValueClass(IntWritable.class);
job.setInputFormatClass(Titanic_input.class);
job.setOutputFormatClass(TextOutputFormat.class);
 
Path out=new Path(args[1]);
 
out.getFileSystem(conf).delete(out);
 
FileInputFormat.addInputPath(job,new Path( args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
job.waitForCompletion(true);
 
}

}