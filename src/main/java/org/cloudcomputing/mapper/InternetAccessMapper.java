package org.cloudcomputing.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InternetAccessMapper extends Mapper<LongWritable, Text, Text, Text> {
  private final Text keyOut = new Text();
  private final Text valOut = new Text();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(",");
    if (fields[0].equals("Age")) return;

    try {
      String schoolType = fields[6];   // Column 7: SchoolType
      String locale = fields[7];       // Column 8: Locale
      String internet = fields[14];    // Column 14: InternetAccess (Yes/No)

      String combinedKey = schoolType + "_" + locale;
      String accessType = internet.equalsIgnoreCase("1") ? "InternetAccess" : "NoAccess";

      keyOut.set(combinedKey);
      valOut.set(accessType);
      context.write(keyOut, valOut);
    } catch (Exception ignored) {}
  }
}
