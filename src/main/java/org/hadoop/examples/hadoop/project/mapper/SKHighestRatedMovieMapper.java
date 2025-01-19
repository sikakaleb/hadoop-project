package org.hadoop.examples.hadoop.project.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SKHighestRatedMovieMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text userID = new Text();
    private Text movieAndRating = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("userId") || line.isEmpty()) { // Ignore header and empty lines
            return;
        }

        String[] fields = line.split(",");
        if (fields.length > 2) {
            userID.set(fields[0]); // userId
            movieAndRating.set(fields[1] + ":" + fields[2]); // movieId:rating
            context.write(userID, movieAndRating);
        }
    }
}

