package com.cloudxlab.titanic;
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
 
import org.apache.hadoop.mapreduce.InputSplit;
 
import org.apache.hadoop.mapreduce.RecordReader;
 
import org.apache.hadoop.mapreduce.TaskAttemptContext;
 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
public class Titanic_input extends FileInputFormat<Key_value,IntWritable> {
 
@Override
 
public RecordReader<Key_value,IntWritable> createRecordReader(InputSplit arg0,
 
TaskAttemptContext arg1) throws IOException, InterruptedException {
 
return new MyRecordReader();
 
}
 
}