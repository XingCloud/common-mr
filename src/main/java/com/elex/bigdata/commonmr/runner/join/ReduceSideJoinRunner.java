package com.elex.bigdata.commonmr.runner.join;

import com.elex.bigdata.commonmr.mapper.join.ReduceSideJoinMapper;
import com.elex.bigdata.commonmr.reducer.join.ReduceSideJoinReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * User: Z J Wu Date: 14-2-21 Time: 下午4:42 Package: com.elex.bigdata.commonmr.runner.join
 */
public class ReduceSideJoinRunner {

  private static final Logger LOGGER = Logger.getLogger(ReduceSideJoinRunner.class);

  public static void main(String[] args) throws IOException {
    if (args == null || args.length < 3) {
      System.out.println("Wrong parameter number.");
      return;
    }

    String inputLeft = args[0];
    String inputRight = args[1];

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(ReduceSideJoinRunner.class);
    job.setJobName("ReduceSideJoin(" + inputLeft + ", " + inputRight + ")");

    job.setMapperClass(ReduceSideJoinMapper.class);
    job.setReducerClass(ReduceSideJoinReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(inputLeft));
    FileInputFormat.addInputPath(job, new Path(inputRight));
//    MultipleInputs.addInputPath(job, new Path(inputLeft), TextInputFormat.class, ReduceSideJoinMapper.class);
//    MultipleInputs.addInputPath(job, new Path(inputRight), TextInputFormat.class, ReduceSideJoinMapper.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    try {
      job.waitForCompletion(true);
    } catch (Exception e) {
      LOGGER.error(e);
    }
  }
}
