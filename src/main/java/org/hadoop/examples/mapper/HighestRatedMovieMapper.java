package org.hadoop.examples.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighestRatedMovieMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text userID = new Text();
    private Text movieAndRating = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length > 2) {
            userID.set(fields[0]);
            movieAndRating.set(fields[1] + ":" + fields[2]);
            context.write(userID, movieAndRating);
        }
    }
}

