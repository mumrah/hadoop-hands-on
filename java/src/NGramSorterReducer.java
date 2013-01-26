package ngram;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NGramSorterReducer 
    implements Reducer<NGramWithIntWritable, NullWritable, NGramWritable, IntWritable> {
  public static final String LIMIT = NGramSorterReducer.class.getName() + ".limit";
  public static final int DEFAULT_LIMIT = 100;

  private int limit;
  private volatile int count;

  @Override
  public void configure(JobConf conf) { 
    limit = conf.getInt(LIMIT, DEFAULT_LIMIT);
    count = 0;
  }

  @Override
  public void reduce(NGramWithIntWritable ngramWithInt, Iterator<NullWritable> _,
      OutputCollector<NGramWritable, IntWritable> collector, Reporter reporter) throws IOException {
    if(count < limit) {
      collector.collect(ngramWithInt.getNGramWritable(), ngramWithInt.getIntWritable());
      count++;
    }
  }

  @Override
  public void close() throws IOException { /* Nothing to do here */ }
}
