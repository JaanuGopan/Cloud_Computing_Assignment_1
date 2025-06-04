package org.cloudcomputing.mapper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GPAClassCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text keyOut = new Text();
    private static final IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields[0].equals("Age")) return;

        try {
            String gender = fields[2];      // Column 3
            String schoolType = fields[6];  // Column 7
            double gpa = Double.parseDouble(fields[11]); // Column 12

            String gpaClass;
            if (gpa > 3.7)
                gpaClass = "FirstClass";
            else if (gpa > 3.5)
                gpaClass = "SecondUpper";
            else if (gpa > 3.0)
                gpaClass = "SecondLower";
            else
                gpaClass = "Normal";

            keyOut.set(gender + "_" + schoolType + "_" + gpaClass); // e.g., Female_Private_FirstClass
            context.write(keyOut, one);
        } catch (Exception e) {}
    }
}