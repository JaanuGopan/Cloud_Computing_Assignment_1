package org.cloudcomputing.mapper;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class GPAClassMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text keyOut = new Text();
    private static final Text one = new Text("1");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length < 12 || fields[0].equalsIgnoreCase("Age")) return;

        try {
            String gender = fields[2].trim();      // Column 3
            String schoolType = fields[6].trim();  // Column 7
            double gpa = Double.parseDouble(fields[11].trim()); // Column 12

            String gpaClass;
            if (gpa > 3.7)
                gpaClass = "FirstClass";
            else if (gpa > 3.5)
                gpaClass = "SecondUpper";
            else if (gpa > 3.0)
                gpaClass = "SecondLower";
            else
                gpaClass = "Normal";

            // Output key: Gender_SchoolType_GPAClass
            keyOut.set(gender + "_" + schoolType + "_" + gpaClass);
            context.write(keyOut, one);
        } catch (Exception ignored) {}
    }
}
