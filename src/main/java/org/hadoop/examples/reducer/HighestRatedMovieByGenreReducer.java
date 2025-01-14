package org.hadoop.examples.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HighestRatedMovieByGenreReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Float> movieRatings = new HashMap<>();
        String genre = key.toString();

        for (Text value : values) {
            String val = value.toString();
            if (val.startsWith("RATING:")) {
                String[] parts = val.substring(7).split(":");
                if (parts.length == 2) { // Ensure there are exactly 2 parts
                    movieRatings.put(parts[0], Float.parseFloat(parts[1]));
                }
            }
        }

        String highestRatedMovie = null;
        float highestRating = 0;

        for (Map.Entry<String, Float> entry : movieRatings.entrySet()) {
            if (entry.getValue() > highestRating) {
                highestRating = entry.getValue();
                highestRatedMovie = entry.getKey();
            }
        }

        if (highestRatedMovie != null) {
            context.write(new Text(genre), new Text(highestRatedMovie + " (Rating: " + highestRating + ")"));
        }
    }
}