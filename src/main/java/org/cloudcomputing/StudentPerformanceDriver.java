package org.cloudcomputing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.cloudcomputing.tableformatter.mapper.FreeTimePartTimeGPAMapper;
import org.cloudcomputing.tableformatter.mapper.GPAClassMapper;
import org.cloudcomputing.tableformatter.mapper.InternetAccessMapper;
import org.cloudcomputing.tableformatter.mapper.InternetRelationshipGPAMapper;
import org.cloudcomputing.tableformatter.reducer.FreeTimePartTimeGPAReducer;
import org.cloudcomputing.tableformatter.reducer.GPAClassReducer;
import org.cloudcomputing.tableformatter.reducer.InternetAccessReducer;
import org.cloudcomputing.tableformatter.reducer.InternetRelationshipGPAReducer;

public class StudentPerformanceDriver {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: Main <input path> <output base path>");
      System.exit(-1);
    }

    Path inputPath = new Path(args[0]);
    String outputBase = args[1];

    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1, "GPA Class Distribution by Gender and SchoolType");

    job1.setJarByClass(StudentPerformanceDriver.class);
    job1.setMapperClass(GPAClassMapper.class);
    job1.setReducerClass(GPAClassReducer.class);
    job1.setNumReduceTasks(1);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job1, inputPath);
    FileOutputFormat.setOutputPath(job1, new Path(outputBase + "/table/job1_GenderSchoolType_GPAClass"));

    if (!job1.waitForCompletion(true)) {
      System.exit(1);
    }

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Internet Access by SchoolType and Locale");

    job2.setJarByClass(StudentPerformanceDriver.class);
    job2.setMapperClass(InternetAccessMapper.class);
    job2.setReducerClass(InternetAccessReducer.class);
    job2.setNumReduceTasks(1);

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2, inputPath);
    FileOutputFormat.setOutputPath(job2, new Path(outputBase + "/table/job2_SchoolTypeLocale_InternetAccess"));

    if (!job2.waitForCompletion(true)) {
      System.exit(1);
    }

    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "Pivot FreeTime+PartTime vs GPA");

    job3.setJarByClass(StudentPerformanceDriver.class);
    job3.setMapperClass(FreeTimePartTimeGPAMapper.class);
    job3.setReducerClass(FreeTimePartTimeGPAReducer.class);
    job3.setNumReduceTasks(1);

    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job3, inputPath);
    FileOutputFormat.setOutputPath(job3, new Path(outputBase + "/table/job3_FreeTimePartTime_Pivot"));

    if (!job3.waitForCompletion(true)) {
      System.exit(1);
    }

    Configuration conf4 = new Configuration();
    Job job4 = Job.getInstance(conf4, "GPA Class by Internet Access and Relationship");

    job4.setJarByClass(StudentPerformanceDriver.class);
    job4.setMapperClass(InternetRelationshipGPAMapper.class);
    job4.setReducerClass(InternetRelationshipGPAReducer.class);
    job4.setNumReduceTasks(1);

    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job4, inputPath);
    FileOutputFormat.setOutputPath(job4, new Path(outputBase + "/table/job4_InternetRelationship_GPAClass"));

    if (!job4.waitForCompletion(true)) {
      System.exit(1);
    }

    System.exit(0);
  }
}
