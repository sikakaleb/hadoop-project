package org.hadoop.examples.hadoop.project.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SKHighestRatedMovieMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text userId = new Text();
    private Text movieRating = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Ignore the header or invalid lines
        if (line.startsWith("userId") || line.isEmpty()) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length >= 3) {
            userId.set(fields[0]); // userId
            movieRating.set(fields[1] + ":" + fields[2]); // movieId:rating
            context.write(userId, movieRating);
        }
    }
}
