package org.cloudcomputing.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GenderSchoolTypeGPAMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text keyOut = new Text();
    private DoubleWritable valOut = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields[0].equals("Age")) return; // Skip header

        try {
            String gender = fields[2];       // Column 3: Gender
            String schoolType = fields[7];   // Column 8: SchoolType
            double gpa = Double.parseDouble(fields[13]); // Column 14: GPA

            keyOut.set(gender + "_" + schoolType); // e.g., Male_Public
            valOut.set(gpa);                       // GPA
            context.write(keyOut, valOut);
        } catch (Exception e) {}
    }
}

