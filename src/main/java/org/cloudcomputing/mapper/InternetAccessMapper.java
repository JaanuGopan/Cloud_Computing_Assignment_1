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
    if (fields[0].equals("Age")) return; // Skip header

    try {
      String schoolType = fields[6];   // Column 7: SchoolType
      String locale = fields[7];       // Column 8: Locale
      String internet = fields[14];    // Column 14: InternetAccess (Yes/No)

      String access = internet.equalsIgnoreCase("1") ? "InternetAccess" : "NoAccess";
      keyOut.set(schoolType + "_" + locale + "_" + access); // e.g., Public_City_InternetAccess

      context.write(keyOut, one); // Emit key with count 1
    } catch (Exception e) {
      // Skip malformed lines
    }
  }
}