package org.hadoop.examples.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class SimpleReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> lines = new ArrayList<>();
        for (Text value : values) {
            lines.add(value.toString());
        }
        context.write(key, new Text(lines.toString()));
    }
}

