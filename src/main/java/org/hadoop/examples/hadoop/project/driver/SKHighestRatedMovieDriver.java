package org.hadoop.examples.hadoop.project.driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.hadoop.examples.hadoop.project.mapper.SKHighestRatedMovieMapper;
import org.hadoop.examples.hadoop.project.reducer.SKHighestRatedMovieReducer;

public class SKHighestRatedMovieDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SKHighestRatedMovieDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Find Highest Rated Movie Per User");

        job.setJarByClass(SKHighestRatedMovieDriver.class);
        job.setMapperClass(SKHighestRatedMovieMapper.class);
        job.setReducerClass(SKHighestRatedMovieReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
