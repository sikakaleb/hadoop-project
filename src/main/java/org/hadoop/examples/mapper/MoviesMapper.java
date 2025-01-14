package org.hadoop.examples.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MoviesMapper extends Mapper<Object, Text, Text, Text> {
    private Text movieId = new Text();
    private Text movieTitle = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",", 3);
        // Ignorer la première ligne ou les lignes mal formées
        if (!fields[0].equals("movieId") && fields.length >= 3) {
            movieId.set(fields[0]); // movieId
            movieTitle.set("MOVIE:" + fields[1]); // MOVIE:movieTitle
            context.write(movieId, movieTitle);
        }
    }
}
