package com.cloudxlab.titanic;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;
import com.google.common.collect.ComparisonChain;
 
public class Key_value implements WritableComparable<Key_value> {

private String x;
private String y;
 
public String getX() {
 
return x;
 
}
 
public void setX(String x) {
 
this.x = x;
 
}
 
public String getY() {
 
return y;
 
}
 
public void setY(String y) {
 
this.y = y;
 
}
 
public Key_value(String x, String y) {
 
this.x = x;
 
this.y = y;
 
}
 
public void write(DataOutput out) throws IOException {
 
out.writeUTF(x);
 
out.writeUTF(y);
 
}
 
public void readFields(DataInput in) throws IOException {
 
x = in.readUTF();
 
y = in.readUTF();
 
}
 
public Key_value(){
 
}
 
@Override
 
public int compareTo(Key_value o) {
 
// TODO Auto-generated method stub
 
return ComparisonChain.start().compare(this.y,o.y).compare(this.x,o.x).result();
 
}
 
public boolean equals(Object o1) {
 
if (!(o1 instanceof Key_value)) {
 
return false;
 
}
 
Key_value other = (Key_value)o1;
 
return this.x == other.x && this.y == other.y;
 
}
 
@Override
 
public String toString() {
 
return x.toString()+","+y.toString();
 
}
 
}