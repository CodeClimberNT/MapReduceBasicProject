package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
        Text, // Input value type
        Text, // Output key type
        Text> {// Output value type

    protected void map(
            LongWritable key, // Input key type
            Text value, // Input value type
            Context context) throws IOException, InterruptedException {
        // input: sentenceNumber\twords
        // Split each sentence in sentence number and words. Use tab and whitespace(s)
        // as delimiter
        String sentenceNumber = value.toString().split("\t")[0];
        String[] words = value.toString().split("\t")[1].split(" ");

        // Iterate over the set of words
        for (String word : words) {
            // Transform word case
            word = word.toLowerCase();

            if (word.contains("and") || word.contains("or") || word.contains("not")) {
                continue;
            }

            // emit the pair (word, sentenceNumber)
            context.write(new Text(word),
                    new Text(sentenceNumber));
        }
    }
}
