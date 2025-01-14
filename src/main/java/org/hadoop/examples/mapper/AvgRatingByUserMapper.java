package org.hadoop.examples.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgRatingByUserMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private Text userID = new Text();
    private FloatWritable rating = new FloatWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Ignore header or invalid lines
        if (line.startsWith("userId") || line.isEmpty()) {
            return;
        }
        String[] fields = line.split(",");
        if (fields.length > 2) {
            userID.set(fields[0]);
            rating.set(Float.parseFloat(fields[2]));
            context.write(userID, rating);
        }
    }
}
