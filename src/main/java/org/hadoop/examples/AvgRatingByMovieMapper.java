package org.hadoop.examples;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgRatingByMovieMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private Text movieID = new Text();
    private FloatWritable rating = new FloatWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length > 2) {
            movieID.set(fields[1]);
            rating.set(Float.parseFloat(fields[2]));
            context.write(movieID, rating);
        }
    }
}

