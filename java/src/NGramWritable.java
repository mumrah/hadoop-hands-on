package ngram;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class NGramWritable extends ArrayWritable
    implements WritableComparable<NGramWritable> {

  public NGramWritable() {
    super(Text.class);
  }

  public void set(String... ngrams) {
    Text[] texts = new Text[ngrams.length];
    for(int i=0; i<ngrams.length; i++){
      texts[i] = new Text(ngrams[i]);
    }
    super.set(texts);
  }

  @Override
  public String toString() {
    String out = "(";
    for(Writable w: this.get()) {
      out += w.toString() + ", ";
    }
    return out.substring(0, out.length()-2) + ")";
  }

  @Override
  public int compareTo(NGramWritable other) {
    Writable[] w1 = this.get();
    Writable[] w2 = other.get();
    int cmp = 0;
    int len = Math.min(w1.length, w2.length);
    // Compare Text objects one by one
    for(int i=0; i<len; i++) {
      if(cmp == 0) {
        cmp = ((Text)(w1[i])).compareTo((Text)(w2[i]));
      } else {
        break;
      }
    }
    // If all ngrams are equal, the tie breaker is based on the length
    if(cmp == 0) { 
      cmp = w1.length - w2.length;
    }
    return cmp;
  }

  @Override
  public boolean equals(Object other) {
    if(other instanceof NGramWritable) {
      return this.compareTo((NGramWritable)other) == 0;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.get());
  }
}
