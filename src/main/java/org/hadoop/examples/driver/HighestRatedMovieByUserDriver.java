package org.hadoop.examples.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hadoop.examples.mapper.MoviesMapper;
import org.hadoop.examples.mapper.RatingsMapper;
import org.hadoop.examples.reducer.HighestRatedMovieByUserReducer;

public class HighestRatedMovieByUserDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: HighestRatedMovieByUserDriver <ratings input> <movies input> <output>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(HighestRatedMovieByUserDriver.class);
        job.setJobName("Highest Rated Movie By User");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MoviesMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setReducerClass(HighestRatedMovieByUserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

