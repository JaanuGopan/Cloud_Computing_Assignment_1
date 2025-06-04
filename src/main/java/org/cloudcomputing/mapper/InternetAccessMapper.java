package org.cloudcomputing.mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InternetAccessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final Text keyOut = new Text();
  private final static IntWritable one = new IntWritable(1);

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(",");
    if (fields[0].equals("Age")) return;

    try {
      String schoolType = fields[6];
      String locale = fields[7];
      String internet = fields[14];

      String access = internet.equalsIgnoreCase("1") ? "InternetAccess" : "NoAccess";
      keyOut.set(schoolType + "_" + locale + "_" + access);

      context.write(keyOut, one);
    } catch (Exception ignored) {}
  }
}