package org.hadoop.examples.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighestRatedTagReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String tagName = null;
        float maxRelevance = 0;

        for (Text value : values) {
            String val = value.toString();
            if (val.startsWith("TAG:")) {
                tagName = val.substring(4);
            } else if (val.startsWith("SCORE:")) {
                float relevance = Float.parseFloat(val.substring(6));
                if (relevance > maxRelevance) {
                    maxRelevance = relevance;
                }
            }
        }

        if (tagName != null) {
            context.write(new Text(tagName), new Text("Relevance: " + maxRelevance));
        }
    }
}

