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
        // Ignorer les lignes vides
        if (line.isEmpty()) {
            return;
        }

        // Split par tabulation
        String[] fields = line.split("\t");
        if (fields.length >= 2) {
            movieId.set(fields[1].split(" ")[0]); // Récupérer seulement le movieId
            userInfo.set("USER:" + fields[0]);    // Récupérer le userId
            context.write(movieId, userInfo);
        }
    }
}
