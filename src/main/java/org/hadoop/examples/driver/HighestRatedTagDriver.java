package org.hadoop.examples.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hadoop.examples.mapper.GenomeScoresMapper;
import org.hadoop.examples.mapper.GenomeTagsMapper;
import org.hadoop.examples.reducer.HighestRatedTagReducer;

public class HighestRatedTagDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: HighestRatedTagDriver <genome scores input> <genome tags input> <output>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(HighestRatedTagDriver.class);
        job.setJobName("Highest Rated Tag");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, GenomeScoresMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, GenomeTagsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setReducerClass(HighestRatedTagReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

