package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;

public class CustomStringArrayWritable extends ArrayWritable {
    private String[] strings;

    public CustomStringArrayWritable(String[] values) {
        super(values);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        strings = in.readLine().split("\t");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(strings[0]);
    }

}
