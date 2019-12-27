package me.train.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class HashCode extends ScalarFunction {
    private int factor = 12;

    public HashCode(int factor) {
        this.factor = factor;
    }

    public int eval(String s) {
        return s.hashCode() * factor;
    }
}

