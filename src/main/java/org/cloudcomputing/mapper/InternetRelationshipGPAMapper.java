package org.cloudcomputing.mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InternetRelationshipGPAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final Text outputKey = new Text();
  private final static IntWritable one = new IntWritable(1);

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(",");
    if (fields[0].equals("Age")) return;

    try {
      String internetRaw = fields[14];
      String relationshipRaw = fields[18];
      double gpa = Double.parseDouble(fields[11]);

      String internetAccess = internetRaw.equalsIgnoreCase("1")
              ? "InternetAccess" : "NoInternetAccess";
      String relationship = relationshipRaw.equalsIgnoreCase("1")
              ? "InRelationship" : "NoRelationship";

      String gpaCategory;
      if (gpa > 3.7) {
        gpaCategory = "FirstClass";
      } else if (gpa > 3.5) {
        gpaCategory = "SecondUpper";
      } else if (gpa >= 3.0) {
        gpaCategory = "SecondLower";
      } else {
        gpaCategory = "Normal";
      }

      outputKey.set(internetAccess + "_" + relationship + "_" + gpaCategory);
      context.write(outputKey, one);

    } catch (Exception ignored) {}
  }
}