package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
        Text, // Input value type
        Text, // Output key type
        Text> { // Output value type

    @Override

    protected void reduce(
            Text key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {

        String sentenceNumbers = "";
 


        boolean firstSentence = true;
        // Iterate over the set of values and sum them
        for (Text value : values) {
            if (firstSentence) {
                sentenceNumbers = sentenceNumbers.concat(value.toString());
                firstSentence = false;
                continue;
            }
            sentenceNumbers = sentenceNumbers.concat(", ").concat(value.toString());
        }

        context.write(key, new Text(sentenceNumbers));
    }
}
