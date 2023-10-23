package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MinMaxWritable implements Writable {
    private double minValue;
    private double maxValue;

    MinMaxWritable() {
        minValue = Double.MAX_VALUE;
        maxValue = Double.MIN_VALUE;
    }

    MinMaxWritable(double pValue) {
        minValue = pValue;
        maxValue = pValue;
    }

    public void updateValues(double pValue) {
        if (pValue < this.minValue) {
            this.minValue = pValue;
        }
        if (pValue > this.maxValue) {
            this.maxValue = pValue;
        }
    }

    public void updateValues(MinMaxWritable pValue) {
        if (pValue.getMin() < this.minValue) {
            this.minValue = pValue.getMin();
        }
        if (pValue.getMax() > this.maxValue) {
            this.maxValue = pValue.getMax();
        }
    }

    public void updateMinValueIfNeeded(double pValue) {
        if (pValue < this.minValue) {
            this.minValue = pValue;
        }
    }

    public void updateMaxValueIfNeeded(double pValue) {
        if (pValue > this.maxValue) {
            this.maxValue = pValue;
        }
    }

    public void setMinValue(double pValue) {
        this.minValue = pValue;
    }

    public void setmaxValue(double pValue) {
        this.maxValue = pValue;
    }

    public double[] getValues() {
        return new double[] { this.minValue, this.maxValue };
    }

    public double getMin() {
        return this.minValue;
    }

    public double getMax() {
        return this.maxValue;
    }

    @Override
    public String toString() {
        return "max=" + this.maxValue + "_min=" + this.minValue;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        minValue = in.readDouble();
        maxValue = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(minValue);
        out.writeDouble(maxValue);
    }

}
