package org.cloudcomputing.mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FreeTimePartTimeGPAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final Text outputKey = new Text();
  private final static IntWritable one = new IntWritable(1);

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(",");
    // Skip header
    if (fields[0].equals("Age")) return;

    try {
      int freeTime = Integer.parseInt(fields[19]);
      String partTimeJobRaw = fields[16];
      double gpa = Double.parseDouble(fields[11]);

      String freeTimeCategory = (freeTime <= 2) ? "LowFreeTime" : "HighFreeTime";

      String partTimeJob = partTimeJobRaw.equalsIgnoreCase("1") ? "WithPartTimeJob" : "NoPartTimeJob";

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

      outputKey.set(freeTimeCategory + "_" + partTimeJob + "_" + gpaCategory);
      context.write(outputKey, one);
    } catch (Exception ignored) {}
  }
}