IN=../data/pg100.txt.gz
OUT1=../data/ngrams-count
OUT2=../data/ngrams-sorted
N=2

default:

clean:
	rm -rf ${OUT1}
	rm -rf ${OUT2}
	rm -f ngram-job.jar
	rm -rf build

jar:
	mkdir -p build
	javac -cp .:`../hadoop-1.0.4/bin/hadoop classpath` -d build src/*.java
	jar cvf ngram-job.jar -C build ngram
	rm -rf build

test: jar
	java -ea -cp .:`../hadoop-1.0.4/bin/hadoop classpath`:ngram-job.jar ngram.Test

run: jar
	java -Xmx1g -cp .:`../hadoop-1.0.4/bin/hadoop classpath`:ngram-job.jar ngram.NGramCountJob ${IN} ${OUT1} ${OUT2} ${N}
	cat ${OUT2}/part-*
