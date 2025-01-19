package org.hadoop.examples.hadoop.project.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SKUserRatingsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text movieId = new Text();
    private Text userInfo = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Ignorer les lignes d'entête ou les lignes vides
        if (line.startsWith("userId") || line.isEmpty()) {
            return;
        }
        String[] fields = line.split(",");
        if (fields.length >= 2) {
            movieId.set(fields[1]);
            userInfo.set("USER:" + fields[0]);
            context.write(movieId, userInfo);
        }
    }
}
