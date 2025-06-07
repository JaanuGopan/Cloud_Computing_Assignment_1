package org.cloudcomputing.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.cloudcomputing.enums.FreeTime;
import org.cloudcomputing.enums.GPACategory;
import org.cloudcomputing.enums.PartTimeJob;
import org.cloudcomputing.model.StudentRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class FreeTimePartTimeGPAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text outputKey = new Text();
    private final static IntWritable one = new IntWritable(1);

    // Custom counter to track invalid or malformed records
    enum CountersEnum {MALFORMED_RECORDS}

    private static final Log LOG = LogFactory.getLog(FreeTimePartTimeGPAMapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context) {
        try {
            StudentRecord studentRecord = new StudentRecord(value);
            int freeTime = studentRecord.freeTime;
            boolean partTimeJobStatus = studentRecord.partTimeJob;
            double gpa = studentRecord.gpa;

            // Categorize free time
            FreeTime freeTimeCategory = FreeTime.fromFreeTime(freeTime);

            // Categorize based on part-time job
            PartTimeJob partTimeJobCategory = PartTimeJob.fromBoolean(partTimeJobStatus);

            // Categorize GPA into academic performance levels
            GPACategory gpaCategory = GPACategory.fromGPA(gpa);

            // Build the output key using the categories
            outputKey.set(freeTimeCategory.name() + "_" + partTimeJobCategory.name() + "_" + gpaCategory.name());

            // Emit the key with a count of 1 for each student record
            context.write(outputKey, one);

        } catch (Exception e) {
            // If parsing fails, log the bad record and increment the malformed counter
            LOG.warn("Skipping bad record: " + value);
            context.getCounter(CountersEnum.MALFORMED_RECORDS).increment(1);
        }
    }
}
