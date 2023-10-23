package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class CombinerBigData extends Reducer<Text, // Input key type
        MinMaxWritable, // Input value type
        Text, // Output key type
        MinMaxWritable> { // Output value type

    @Override

    protected void reduce(
            Text key, // Input key type
            Iterable<MinMaxWritable> temperatures, // Input value type
            Context context) throws IOException, InterruptedException {
        MinMaxWritable temp = new MinMaxWritable();

        // Iterate over the set of temperatures and update min and max
        for (MinMaxWritable currentTemp : temperatures) {
            temp.updateValues(currentTemp);
        }

        context.write(key, temp);
    }
}
