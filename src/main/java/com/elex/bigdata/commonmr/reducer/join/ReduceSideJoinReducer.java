package com.elex.bigdata.commonmr.reducer.join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: Z J Wu Date: 14-2-21 Time: 下午4:39 Package: com.elex.bigdata.commonmr.reducer.join
 */
public class ReduceSideJoinReducer extends Reducer<Text, Text, Text, NullWritable> {
  @Override protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
    InterruptedException {

    int sum = 0;
    for (Text t : values) {
      ++sum;
    }

    if (sum == 2) {
      context.write(key, NullWritable.get());
    }
  }
}
