package org.cloudcomputing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.cloudcomputing.mapper.FreeTimePartTimeGPAMapper;
import org.cloudcomputing.mapper.GPAClassCountMapper;
import org.cloudcomputing.mapper.InternetAccessMapper;
import org.cloudcomputing.mapper.InternetRelationshipGPAMapper;
import org.cloudcomputing.reducer.GPACategotyReducer;
import org.cloudcomputing.reducer.InternetAccessReducer;

public class StudentPerformanceMapReduceWithPivot {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: Main <input path> <output base path>");
      System.exit(-1);
    }

    Path inputPath = new Path(args[0]);
    String outputBase = args[1];

    // Job 1 : GPA Class Distribution by Gender and SchoolType
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "GPA Class Distribution by Gender and SchoolType");

    job1.setJarByClass(StudentPerformanceMapReduceWithPivot.class);
    job1.setMapperClass(GPAClassCountMapper.class);
    job1.setReducerClass(GPACategotyReducer.class);
    job1.setNumReduceTasks(1);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job1, inputPath);
    FileOutputFormat.setOutputPath(job1, new Path(outputBase + "/table/job1_GenderSchoolType_GPAClass"));

    if (!job1.waitForCompletion(true)) {
      System.exit(1);
    }

    // Job 2 : Internet Access Count by SchoolType and Locale
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Internet Access by SchoolType and Locale");

    job2.setJarByClass(StudentPerformanceMapReduceWithPivot.class);
    job2.setMapperClass(InternetAccessMapper.class);
    job2.setReducerClass(InternetAccessReducer.class);
    job2.setNumReduceTasks(1);

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job2, inputPath);
    FileOutputFormat.setOutputPath(job2, new Path(outputBase + "/table/job2_SchoolTypeLocale_InternetAccess"));

    if (!job2.waitForCompletion(true)) {
      System.exit(1);
    }

    // Job 3: FreeTime and PartTimeJob vs GPA
    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "FreeTime and PartTime vs GPA");

    job3.setJarByClass(StudentPerformanceMapReduceWithPivot.class);
    job3.setMapperClass(FreeTimePartTimeGPAMapper.class);
    job3.setReducerClass(GPACategotyReducer.class);
    job3.setNumReduceTasks(1);

    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job3, inputPath);
    FileOutputFormat.setOutputPath(job3, new Path(outputBase + "/table/job3_FreeTimePartTime_Pivot"));

    if (!job3.waitForCompletion(true)) {
      System.exit(1);
    }

    // Job 4: GPA Class by Internet Access and Relationship
    Configuration conf4 = new Configuration();
    Job job4 = Job.getInstance(conf4, "GPA Class by Internet Access and Relationship");

    job4.setJarByClass(StudentPerformanceMapReduceWithPivot.class);
    job4.setMapperClass(InternetRelationshipGPAMapper.class);
    job4.setReducerClass(GPACategotyReducer.class);
    job4.setNumReduceTasks(1);

    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job4, inputPath);
    FileOutputFormat.setOutputPath(job4, new Path(outputBase + "/table/job4_InternetRelationship_GPAClass"));

    if (!job4.waitForCompletion(true)) {
      System.exit(1);
    }

    System.exit(0);
  }
}
