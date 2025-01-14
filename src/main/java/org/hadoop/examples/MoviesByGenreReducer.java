package org.hadoop.examples;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MoviesByGenreReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder movieList = new StringBuilder();
        for (Text value : values) {
            movieList.append(value.toString()).append(", ");
        }
        context.write(key, new Text(movieList.toString()));
    }
}

