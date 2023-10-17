package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
        DoubleWritable, // Input value type
        Text, // Output key type
        Text> { // Output value type

    @Override

    protected void reduce(
            Text key, // Input key type
            Iterable<DoubleWritable> temperatures, // Input value type
            Context context) throws IOException, InterruptedException {

        // take track of max min and value found during the program
        Double maxRecord = Double.MIN_VALUE;
        Double minRecord = Double.MAX_VALUE;

        // Iterate over the set of temperatures and sum them
        for (DoubleWritable currentTemp : temperatures) {
            if (currentTemp.get() > maxRecord) {
                maxRecord = currentTemp.get();
            }

            if (currentTemp.get() < minRecord) {
                minRecord = currentTemp.get();
            }
        }

        context.write(key, new Text("max=" + maxRecord + "_min=" + minRecord));
    }
}
