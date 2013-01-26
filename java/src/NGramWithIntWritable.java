package ngram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class NGramWithIntWritable
    implements WritableComparable<NGramWithIntWritable> {
  
  private NGramWritable ngramW;
  private IntWritable intW;

  public NGramWithIntWritable() {
    ngramW = new NGramWritable();
    intW = new IntWritable();
  }

  public NGramWithIntWritable(NGramWritable ngramW, IntWritable intW) {
    this.ngramW = ngramW;
    this.intW = intW;
  }

  public NGramWritable getNGramWritable() {
    return ngramW;
  }

  public IntWritable getIntWritable() {
    return intW;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    intW.write(out);
    ngramW.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    intW.readFields(in);
    ngramW.readFields(in);
  }

  @Override
  public int compareTo(NGramWithIntWritable other) {
    return -intW.compareTo(other.intW); // reverse the order
  }

  @Override
  public boolean equals(Object other) {
    if(other instanceof NGramWithIntWritable) {
      return this.compareTo((NGramWithIntWritable)other) == 0;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return intW.hashCode();
  }

  @Override
  public String toString() {
    return "(" + ngramW.toString() + ", " + intW.toString() + ")";
  }

}
