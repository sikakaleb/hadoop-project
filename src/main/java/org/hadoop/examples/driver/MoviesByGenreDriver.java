package org.hadoop.examples.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hadoop.examples.mapper.MoviesByGenreMapper;
import org.hadoop.examples.reducer.MoviesByGenreReducer;

public class MoviesByGenreDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: MoviesByGenreDriver <input> <output> <genre>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("genre.filter", args[2]);

        Job job = Job.getInstance(conf, "List Movies By Genre");

        job.setJarByClass(MoviesByGenreDriver.class);
        job.setMapperClass(MoviesByGenreMapper.class);
        job.setReducerClass(MoviesByGenreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

