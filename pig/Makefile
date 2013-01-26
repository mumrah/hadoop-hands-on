IN=../data/pg100.txt.gz
OUT=../data/ngrams-pig-sorted
N=2

default:


clean:
	rm -f ngram-udf.jar
	rm -rf build
	rm -rf ${OUT}
	rm -f pig_*.log

jar:
	mkdir -p build
	javac -cp .:pig-0.10.1-withouthadoop.jar:`../hadoop-1.0.4/bin/hadoop classpath` -d build src/*.java
	jar cvf ngram-udf.jar -C build org
	rm -rf build

run: jar
	java -Xmx1g -cp .:pig-0.10.1-withouthadoop.jar:`../hadoop-1.0.4/bin/hadoop classpath` \
		org.apache.pig.Main -x local -p in=${IN} -p out=${OUT} top-100-ngrams.pig
	cat ${OUT}/part-*
