package org.cloudcomputing.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.cloudcomputing.enums.InternetStatus;
import org.cloudcomputing.model.StudentRecord;

public class InternetAccessMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final Text keyOut = new Text();
  private final static IntWritable one = new IntWritable(1);
  private static final Log LOG = LogFactory.getLog(InternetAccessMapper.class);

  // Enum for tracking malformed input
  enum CountersEnum { MALFORMED_RECORDS }

  public void map(LongWritable key, Text value, Context context) {
    try {
      StudentRecord studentRecord = new StudentRecord(value);

      String schoolType = studentRecord.schoolType;
      String locale = studentRecord.locale;
      boolean internetStatus = studentRecord.internetAccess;

      // Categorize internet access
      InternetStatus internetAccess = InternetStatus.fromBoolean(internetStatus);

      // Build the output key using the categories
      keyOut.set(schoolType + "_" + locale + "_" + internetAccess.name());

      // Emit the key with a count of 1 for each student record
      context.write(keyOut, one);
    } catch (Exception e) {
      // Skip header and malformed rows
      LOG.warn("Skipping record: " + value);
      context.getCounter(CountersEnum.MALFORMED_RECORDS).increment(1);
    }
  }
}