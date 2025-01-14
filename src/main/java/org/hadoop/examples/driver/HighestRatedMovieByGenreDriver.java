package org.hadoop.examples.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hadoop.examples.mapper.MoviesGenreMapper;
import org.hadoop.examples.mapper.RatingsByGenreMapper;
import org.hadoop.examples.reducer.HighestRatedMovieByGenreReducer;

public class HighestRatedMovieByGenreDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: HighestRatedMovieByGenreDriver <movies input> <ratings input> <output>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(HighestRatedMovieByGenreDriver.class);
        job.setJobName("Highest Rated Movie By Genre");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MoviesGenreMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingsByGenreMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setReducerClass(HighestRatedMovieByGenreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

