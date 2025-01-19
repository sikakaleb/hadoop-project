package org.hadoop.examples.hadoop.project.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SKMoviesJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String movieTitle = null;
        List<String> userIds = new ArrayList<>();
        System.out.println("Reducer Key: " + key.toString());

        for (Text value : values) {
            System.out.println("Reducer Value: " + value.toString());
            String val = value.toString();
            if (val.startsWith("MOVIE:")) {
                movieTitle = val.substring(6); // Récupérer le titre du film
            } else if (val.startsWith("USER:")) {
                userIds.add(val.substring(5)); // Récupérer l'userId
            }
        }

        if (movieTitle != null) {
            for (String userId : userIds) {
                context.write(new Text(userId), new Text(movieTitle));
            }
        }
    }
}
