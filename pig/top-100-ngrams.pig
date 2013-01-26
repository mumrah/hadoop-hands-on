register 'ngram-udf.jar';

-- Read in the text file(s), convert to lowercase
raw = load '$in' using TextLoader() as (line:chararray);
lowercased = foreach raw generate LOWER(line) as line:chararray;

-- Generate ngrams
tokens = foreach lowercased generate flatten(org.apache.pig.tutorial.NGramGenerator(line)) as ngram;

-- Group and count the ngrams
tokens_grouped = group tokens by ngram;
token_counts = foreach tokens_grouped generate group as token:chararray, COUNT(tokens) as count:long;

-- Sort and limit the ngrams
sorted_tokens = order token_counts by count desc;
top_100_tokens = limit sorted_tokens 100;

-- Store the output
store top_100_tokens into '$out' using PigStorage();

