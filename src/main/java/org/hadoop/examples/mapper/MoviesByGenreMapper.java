package org.hadoop.examples.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MoviesByGenreMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text genreKey = new Text();
    private Text movieTitle = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length > 2) {
            String title = fields[1];
            String[] genres = fields[2].split("\\|");
            for (String genre : genres) {
                if (genre.equalsIgnoreCase(context.getConfiguration().get("genre.filter"))) {
                    genreKey.set(genre);
                    movieTitle.set(title);
                    context.write(genreKey, movieTitle);
                }
            }
        }
    }
}

