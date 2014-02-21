package com.elex.bigdata.commonmr.mapper.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * User: Z J Wu Date: 14-2-21 Time: 下午4:35 Package: com.elex.bigdata.commonmr.mapper.join
 */
public class ReduceSideJoinMapper extends Mapper<Object, Text, Text, Text> {

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString().trim();
    if (line.length() < 1) {
      return;
    }
    InputSplit inputSplit = context.getInputSplit();
    String filename = ((FileSplit) inputSplit).getPath().getName();
    context.write(new Text(line), new Text(filename));
  }
}
