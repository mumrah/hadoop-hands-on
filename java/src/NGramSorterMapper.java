package ngram;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NGramSorterMapper implements Mapper<NGramWritable, IntWritable, NGramWithIntWritable, NullWritable> {

  @Override
  public void configure(JobConf conf) { /* Nothing to do here */ }

  @Override
  public void map(NGramWritable ngram, IntWritable count, 
      OutputCollector<NGramWithIntWritable, NullWritable> collector, Reporter reporter) throws IOException {
    collector.collect(new NGramWithIntWritable(ngram, count), NullWritable.get());
  }

  @Override
  public void close() throws IOException { /* Nothing to do here */ }
}
