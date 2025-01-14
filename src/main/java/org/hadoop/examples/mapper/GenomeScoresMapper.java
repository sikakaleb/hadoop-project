package org.hadoop.examples.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GenomeScoresMapper extends Mapper<Object, Text, Text, Text> {
    private Text tagId = new Text();
    private Text relevance = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (!fields[0].equals("movieId") && fields.length >= 3) {
            tagId.set(fields[1]); // tagId
            relevance.set("SCORE:" + fields[2]); // SCORE:relevance
            context.write(tagId, relevance);
        }
    }
}

