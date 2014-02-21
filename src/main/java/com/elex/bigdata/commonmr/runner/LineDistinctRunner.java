package com.elex.bigdata.commonmr.runner;

import com.elex.bigdata.commonmr.mapper.LineDistinctMapper;
import com.elex.bigdata.commonmr.reducer.LineDistinctReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * User: Z J Wu Date: 14-2-21 Time: 下午2:38 Package: com.elex.bigdata.commonmr.runner
 */
public class LineDistinctRunner {
  private static final Logger LOGGER = Logger.getLogger(LineDistinctRunner.class);

  public static void main(String[] args) throws IOException {

    if (args.length < 2) {
      System.out.println("Wrong parameter number.");
      return;
    }

    String inputFilePath = args[0];
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(LineDistinctRunner.class);
    job.setJobName("LineDistinctJob");

    job.setMapperClass(LineDistinctMapper.class);
    job.setReducerClass(LineDistinctReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    MultipleInputs.addInputPath(job, new Path(inputFilePath), TextInputFormat.class, LineDistinctMapper.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    try {
      job.waitForCompletion(true);
    } catch (Exception e) {
      LOGGER.error(e);
    }
  }
}
