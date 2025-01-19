package org.hadoop.examples.hadoop.project.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hadoop.examples.hadoop.project.mapper.SKMoviesMapper;
import org.hadoop.examples.hadoop.project.mapper.SKUserRatingsMapper;
import org.hadoop.examples.hadoop.project.reducer.SKMoviesJoinReducer;

public class SKMoviesJoinDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SKMoviesJoinDriver <movies.csv> <user_ratings> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Join Movies with User Ratings");
        job.setJarByClass(SKMoviesJoinDriver.class);

        // Mappers
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SKMoviesMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SKUserRatingsMapper.class);

        // Reducer
        job.setReducerClass(SKMoviesJoinReducer.class);

        // Outputs
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
