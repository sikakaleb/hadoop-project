package org.hadoop.examples.hadoop.project.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hadoop.examples.hadoop.project.mapper.SKCountUsersPerMovieMapper;
import org.hadoop.examples.hadoop.project.reducer.SKCountUsersPerMovieReducer;

public class SKCountUsersPerMovieDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SKCountUsersPerMovieDriver <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count Users Per Movie");
        job.setJarByClass(SKCountUsersPerMovieDriver.class);

        job.setMapperClass(SKCountUsersPerMovieMapper.class);
        job.setReducerClass(SKCountUsersPerMovieReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
