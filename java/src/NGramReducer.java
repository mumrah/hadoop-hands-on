package ngram;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NGramReducer implements Reducer<NGramWritable, IntWritable, NGramWritable, IntWritable> {
  private final IntWritable intReuse = new IntWritable();

  @Override
  public void configure(JobConf conf) { /* Nothing to do here */ }

  @Override
  public void reduce(NGramWritable ngram, Iterator<IntWritable> counts,
      OutputCollector<NGramWritable, IntWritable> collector, Reporter reporter) throws IOException {
    int total = 0;
    while(counts.hasNext()) {
      total += counts.next().get();
    }
    intReuse.set(total);
    collector.collect(ngram, intReuse);
  }

  @Override
  public void close() throws IOException { /* Nothing to do here */ }
}
