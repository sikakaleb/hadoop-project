package org.hadoop.examples.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingsMapper extends Mapper<Object, Text, Text, Text> {
    private Text movieId = new Text();
    private Text userRating = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        // Ignorer la première ligne ou les lignes mal formées
        if (!fields[0].equals("userId") && fields.length >= 3) {
            movieId.set(fields[1]); // movieId
            userRating.set(fields[0] + ":" + fields[2]); // userId:rating
            context.write(movieId, userRating);
        }
    }
}
