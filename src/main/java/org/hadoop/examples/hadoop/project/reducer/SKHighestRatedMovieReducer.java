package org.hadoop.examples.hadoop.project.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Random;

public class SKHighestRatedMovieReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String highestRatedMovie = null;
        float highestRating = 0;
        Random random = new Random();

        for (Text value : values) {
            String[] parts = value.toString().split(":");
            String movieID = parts[0];
            float rating = Float.parseFloat(parts[1]);

            if (rating > highestRating) {
                highestRating = rating;
                highestRatedMovie = movieID;
            } else if (rating == highestRating && random.nextBoolean()) {
                highestRatedMovie = movieID; // Pick randomly if equal
            }
        }

        if (highestRatedMovie != null) {
            context.write(key, new Text(highestRatedMovie));
        }
    }
}

