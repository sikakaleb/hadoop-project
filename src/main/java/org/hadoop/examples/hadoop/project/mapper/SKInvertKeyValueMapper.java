package org.hadoop.examples.hadoop.project.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SKInvertKeyValueMapper extends Mapper<Object, Text, IntWritable, Text> {
    private IntWritable userCount = new IntWritable();
    private Text movieName = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length == 2) {
            movieName.set(fields[0]);
            userCount.set(Integer.parseInt(fields[1]));
            context.write(userCount, movieName);
        }
    }
}
