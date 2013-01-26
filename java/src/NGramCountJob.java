package ngram;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class NGramCountJob {

  public static void main(String[] args) throws Exception {
    Path inputPath = new Path(args[0]);
    Path intermediatePath = new Path(args[1]);
    Path outputPath = new Path(args[2]);

    JobConf countJob = new JobConf(NGramCountJob.class);
    countJob.setJobName("NGramCount");
    countJob.setJarByClass(NGramCountJob.class);
    countJob.set("fs.default.name", "file:///tmp/hadoop-demo");
    countJob.set("mapred.job.tracker", "local");
    
    // Input
    countJob.setInputFormat(TextInputFormat.class);
    FileInputFormat.setInputPaths(countJob, inputPath);

    // MapReduce
    countJob.setMapperClass(NGramMapper.class);
    countJob.setInt(NGramMapper.NGRAMS, Integer.parseInt(args[3]));
    countJob.setReducerClass(NGramReducer.class);

    // Output
    countJob.setOutputKeyClass(NGramWritable.class);
    countJob.setOutputValueClass(IntWritable.class);
    countJob.setOutputFormat(SequenceFileOutputFormat.class);
    FileOutputFormat.setOutputPath(countJob, intermediatePath);

    RunningJob countJobRun = JobClient.runJob(countJob);
    countJobRun.waitForCompletion();
    if(!countJobRun.isSuccessful()) {
      System.err.println("NGramCount job failed");
      System.exit(1);
    }

    JobConf sortJob = new JobConf(NGramCountJob.class);
    sortJob.setJobName("Sort and Limit");
    sortJob.setJarByClass(NGramCountJob.class);
    sortJob.set("fs.default.name", "file:///tmp/hadoop-demo");
    sortJob.set("mapred.job.tracker", "local");

    sortJob.setInputFormat(SequenceFileInputFormat.class);
    FileInputFormat.setInputPaths(sortJob, intermediatePath);

    sortJob.setMapperClass(NGramSorterMapper.class);
    sortJob.setReducerClass(NGramSorterReducer.class);
    sortJob.setInt(NGramSorterReducer.LIMIT, 100);
    sortJob.setNumReduceTasks(1);

    sortJob.setOutputKeyClass(NGramWithIntWritable.class);
    sortJob.setOutputValueClass(NullWritable.class);
    sortJob.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(sortJob, outputPath);

    RunningJob sortJobRun = JobClient.runJob(sortJob);
    sortJobRun.waitForCompletion();
    if(!sortJobRun.isSuccessful()) {
      System.err.println("Sort/Limit job failed");
      System.exit(1);
    }
  }
}
