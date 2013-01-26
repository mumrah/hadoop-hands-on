package ngram;

public class Test {
  public static void main(String[] args) {
    NGramWritable a = new NGramWritable();
    NGramWritable b = new NGramWritable();
    a.set("hadoop", "is", "really", "cool");
    b.set("hadoop", "is", "really", "cool", "!");
    assert a.compareTo(b) < 0;
    assert a.compareTo(a) == 0;
    assert b.compareTo(a) > 0;
    assert b.compareTo(b) == 0;
    assert a.equals(a);
    assert b.equals(b);

    a.set("hadoop");
    b.set("hadoop!");
    assert a.compareTo(b) < 0;
    assert b.compareTo(a) > 0;

    a.set("hadoop", "one");
    b.set("hadoop", "two");
    assert a.compareTo(b) < 0;
    assert b.compareTo(a) > 0;
  }
}
