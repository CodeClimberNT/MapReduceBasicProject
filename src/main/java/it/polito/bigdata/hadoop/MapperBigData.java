package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
        Text, // Input value type
        Text, // Output key type
        DoubleWritable> {// Output value type

    protected void map(
            LongWritable key, // Input key type
            Text value, // Input value type
            Context context) throws IOException, InterruptedException {


        // input format: id,date,temp
        // Split line and retrieve id and temp
        String sensorId = value.toString().split(",")[0];
        Double temp = Double.parseDouble(value.toString().split(",")[2]);

        // emit the pair (id, temp)
        context.write(new Text(sensorId),
                new DoubleWritable(temp));

    }
}
