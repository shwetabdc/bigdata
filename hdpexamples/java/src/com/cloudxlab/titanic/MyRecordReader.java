package com.cloudxlab.titanic;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
 
public class MyRecordReader extends RecordReader<Key_value,IntWritable> {
private Key_value key;
private IntWritable value;
private LineRecordReader reader = new LineRecordReader();
@Override
 
public void close() throws IOException {
// TODO Auto-generated method stub
reader.close();
}
 
@Override
public Key_value getCurrentKey() throws IOException, InterruptedException {
// TODO Auto-generated method stub
return key;
}
 
@Override
public IntWritable getCurrentValue() throws IOException, InterruptedException {
// TODO Auto-generated method stub
return value;
}
 
@Override
public float getProgress() throws IOException, InterruptedException {
// TODO Auto-generated method stub
return reader.getProgress();
}
 
@Override
public void initialize(InputSplit is, TaskAttemptContext tac)
throws IOException, InterruptedException {
reader.initialize(is, tac);
}
 
@Override
public boolean nextKeyValue() throws IOException, InterruptedException {
// TODO Auto-generated method stub
boolean gotNextKeyValue = reader.nextKeyValue();
if(gotNextKeyValue){
if(key==null){
key = new Key_value();
}
if(value == null){
value = new IntWritable();
}
Text line = reader.getCurrentValue();
String[] tokens = line.toString().split(",");
key.setX(new String(tokens[1]));
key.setY(new String(tokens[4]));
value.set(new Integer(1));
}
else {
key = null;
value = null;
}
return gotNextKeyValue;
}
}