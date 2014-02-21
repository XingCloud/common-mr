package com.elex.bigdata.commonmr.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: Z J Wu Date: 14-2-21 Time: 下午2:30 Package: com.elex.bigdata.commonmr.mapper
 */
public class LineDistinctReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
    InterruptedException {
    context.write(new Text(key.toString()), NullWritable.get());
  }

}
