package org.cloudcomputing.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InternetRelationshipGPAMapper extends Mapper<LongWritable, Text, Text, Text> {
  private final Text outputKey = new Text();
  private final static Text one = new Text("1");

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] fields = value.toString().split(",");
    if (fields[0].equals("Age")) return; // Skip header

    try {
      String internetRaw = fields[14];       // Column 19: InternetAccess (Yes/No)
      String relationshipRaw = fields[18];   // Column 20: RelationshipStatus (Yes/No)
      double gpa = Double.parseDouble(fields[11]); // Column 12: GPA

      String internetAccess = internetRaw.equalsIgnoreCase("1") ? "InternetAccess" : "NoInternetAccess";
      String relationship = relationshipRaw.equalsIgnoreCase("1") ? "InRelationship" : "NoRelationship";

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

    } catch (Exception e) {
      // Ignore parsing errors
    }
  }
}
