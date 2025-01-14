package org.hadoop.examples.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MoviesGenreMapper extends Mapper<Object, Text, Text, Text> {
    private Text genreKey = new Text();
    private Text movieId = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",", 3);
        // Ignorer la première ligne ou les lignes mal formées
        if (!fields[0].equals("movieId") && fields.length >= 3) {
            String movie = fields[0]; // movieId
            String[] genres = fields[2].split("\\|");
            for (String genre : genres) {
                genreKey.set(genre.trim());
                movieId.set(movie);
                context.write(genreKey, movieId);
            }
        }
    }
}
