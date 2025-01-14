package org.hadoop.examples.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GenomeTagsMapper extends Mapper<Object, Text, Text, Text> {
    private Text tagId = new Text();
    private Text tagName = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (!fields[0].equals("tagId") && fields.length >= 2) {
            tagId.set(fields[0]); // tagId
            tagName.set("TAG:" + fields[1]); // TAG:tagName
            context.write(tagId, tagName);
        }
    }
}

