package org.hadoop.examples.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HighestRatedMovieByUserReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String movieTitle = null;
        Map<String, Float> userRatings = new HashMap<>();

        // Parcourt les valeurs pour regrouper les données
        for (Text value : values) {
            String val = value.toString();
            if (val.startsWith("MOVIE:")) {
                movieTitle = val.substring(6); // Extraire le titre
            } else {
                String[] parts = val.split(":");
                String userId = parts[0];
                float rating = Float.parseFloat(parts[1]);
                userRatings.put(userId, rating);
            }
        }

        // Émettre les résultats
        if (movieTitle != null) {
            for (Map.Entry<String, Float> entry : userRatings.entrySet()) {
                context.write(new Text(entry.getKey()), new Text(movieTitle + " (Rating: " + entry.getValue() + ")"));
            }
        }
    }
}

