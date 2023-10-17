package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;


public class CustomTextArrayWritable implements Writable{
    private ArrayList<String> strings;

    @Override
    public void readFields(DataInput in) throws IOException {
        strings.add(in.readLine());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(sum);
        out.writeInt(count);
    }
    
}
