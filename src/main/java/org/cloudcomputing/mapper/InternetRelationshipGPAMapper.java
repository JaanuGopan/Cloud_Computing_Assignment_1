package org.cloudcomputing.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.cloudcomputing.enums.GPACategory;
import org.cloudcomputing.enums.InternetStatus;
import org.cloudcomputing.enums.RelationshipStatus;
import org.cloudcomputing.model.StudentRecord;

public class InternetRelationshipGPAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final Text outputKey = new Text();
  private final static IntWritable one = new IntWritable(1);

  private static final Log LOG = LogFactory.getLog(InternetRelationshipGPAMapper.class);

  // Enum for tracking malformed input
  enum CountersEnum { MALFORMED_RECORDS }

  public void map(LongWritable key, Text value, Context context) {
    try {
      StudentRecord studentRecord = new StudentRecord(value);
      boolean internetAccess = studentRecord.internetAccess;
      boolean relationship = studentRecord.romantic;
      double gpa = studentRecord.gpa;

      // Classify internet access
      InternetStatus internetStatus = InternetStatus.fromBoolean(internetAccess);

      // Classify relationship status
      RelationshipStatus relationshipStatus = RelationshipStatus.fromBoolean(relationship);

      // Classify GPA using shared enum
      GPACategory gpaCategory = GPACategory.fromGPA(gpa);

      // Build the output key using the categories
      outputKey.set(internetStatus.name() + "_" + relationshipStatus.name() + "_" + gpaCategory.name());

      // Emit the key with a count of 1 for each student record
      context.write(outputKey, one);
    } catch (Exception e) {
      // Skip header and malformed rows
      LOG.warn("Skipping record: " + value);
      context.getCounter(CountersEnum.MALFORMED_RECORDS).increment(1);
    }
  }
}