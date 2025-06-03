package org.cloudcomputing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.cloudcomputing.mapper.GenderSchoolTypeGPAMapper;
import org.cloudcomputing.reducer.AverageReducer;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Main <input path> <base output path>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputBase = args[1];

        // Run each job sequentially
        runJob(GenderSchoolTypeGPAMapper.class, inputPath, outputBase + "/job1_Gender_SchoolType_GPA", "Gender_SchoolType → GPA");
    }

    public static void runJob(Class mapperClass, String inputPath, String outputPath, String jobName) throws Exception {
        Configuration conf = new Configuration();

        // Set low memory usage for Mapper, Reducer, and AM to fit local environment
        conf.set("mapreduce.map.memory.mb", "512");
        conf.set("mapreduce.reduce.memory.mb", "512");
        conf.set("yarn.app.mapreduce.am.resource.mb", "512");

        // Optional: run in local mode for testing without YARN
        // conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, jobName);

        job.setJarByClass(Main.class);
        job.setMapperClass(mapperClass);
        job.setReducerClass(AverageReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.out.println("Running Job: " + jobName);
        job.waitForCompletion(true);
    }
}

