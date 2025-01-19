package org.hadoop.examples.hadoop.project.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SKMoviesMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text movieId = new Text();
    private Text movieInfo = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Ignorer les lignes d'entête ou les lignes vides
        if (line.startsWith("movieId") || line.isEmpty()) {
            return;
        }
        String[] fields = line.split(",", 3); // Limiter le split pour ne pas casser les titres avec des virgules
        if (fields.length >= 3) {
            movieId.set(fields[0]);
            movieInfo.set("MOVIE:" + fields[1]);
            context.write(movieId, movieInfo);
        }
    }
}
