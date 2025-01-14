package org.hadoop.examples;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighestRatedMovieReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String highestRatedMovie = null;
        float highestRating = 0;

        for (Text value : values) {
            String[] parts = value.toString().split(":");
            String movieID = parts[0];
            float rating = Float.parseFloat(parts[1]);

            if (rating > highestRating) {
                highestRating = rating;
                highestRatedMovie = movieID;
            }
        }
        context.write(key, new Text(highestRatedMovie + " (Rating: " + highestRating + ")"));
    }
}

