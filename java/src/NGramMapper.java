package ngram;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NGramMapper implements Mapper<LongWritable, Text, NGramWritable, IntWritable> {
  // Some statics to make configuration simple
  public static final String NGRAMS = NGramMapper.class.getName() + ".ngrams";
  public static final int DEFAULT_NGRAMS = 1;

  private final NGramWritable ngramReuse = new NGramWritable();
  private final IntWritable one = new IntWritable(1);
  private int n;

  @Override
  public void configure(JobConf conf) {
    n = conf.getInt(NGRAMS, DEFAULT_NGRAMS);
  }

  @Override
  public void map(LongWritable lineNumber, Text line,
      OutputCollector<NGramWritable, IntWritable> collector, Reporter reporter) throws IOException {
    // Tokenize
    String[] words = tokenize(line.toString());
    if(words.length == 1 && words[0].equals("")) {
      return;
    }
    String[] ngram = new String[n];
    // Iterate through the tokens, emitting n-grams
    for(int i=(n-1); i<words.length; i++) {
      for(int j=0; j<n; j++) {
        ngram[j] = words[i-(n-1-j)];
      }
      ngramReuse.set(ngram);
      // Emit a value to the collector
      collector.collect(ngramReuse, one);
    }
  }

  @Override
  public void close() throws IOException { /* Nothing to do here */ }

  private String[] tokenize(String text) {
    text = text.toLowerCase();
    text = text.replace("'","");
    text = text.replaceAll("[\\s\\W]+", " ").trim();
    return text.split(" ");
  }
}
