package com.elex.bigdata.commonmr.mapper;

import com.elex.bigdata.commonmr.CommonMRConstants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * User: Z J Wu Date: 14-2-21 Time: 下午2:29 Package: com.elex.bigdata.commonmr.mapper
 */
public class LineDistinctMapper extends Mapper<Object, Text, Text, IntWritable> {

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString().trim();
    if (line.length() > 1) {
      context.write(new Text(line), CommonMRConstants.ONE);
    }
  }
}
