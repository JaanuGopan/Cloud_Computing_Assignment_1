package org.cloudcomputing.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudcomputing.enums.GPACategory;
import org.cloudcomputing.model.StudentRecord;

public class GPAClassCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Text keyOut = new Text();
    private static final IntWritable one = new IntWritable(1);
    private static final Log LOG = LogFactory.getLog(GPAClassCountMapper.class);

    // Enum for tracking malformed input
    enum CountersEnum { MALFORMED_RECORDS }

    @Override
    public void map(LongWritable key, Text value, Context context) {
        try {
            StudentRecord studentRecord = new StudentRecord(value);
            String gender = studentRecord.gender;
            String schoolType = studentRecord.schoolType;
            double gpa = studentRecord.gpa;

            // Classify GPA using shared enum
            GPACategory gpaCategory = GPACategory.fromGPA(gpa);

            // Build the output key using the categories
            keyOut.set(gender + "_" + schoolType + "_" + gpaCategory.name());

            // Emit the key with a count of 1 for each student record
            context.write(keyOut, one);
        } catch (Exception e) {
            // Skip header and malformed rows
            LOG.warn("Skipping record: " + value);
            context.getCounter(CountersEnum.MALFORMED_RECORDS).increment(1);
        }
    }
}
