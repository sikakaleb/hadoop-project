package org.hadoop.examples.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMoviesByGenreMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text genre = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");
        if (columns.length < 3) return; // Skip malformed lines
        String[] genres = columns[2].split("\\|");
        for (String g : genres) {
            genre.set(g.trim());
            context.write(genre, one);
        }
    }
}
