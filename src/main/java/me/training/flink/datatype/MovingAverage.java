package me.training.flink.datatype;

import java.util.LinkedList;

public class MovingAverage {

    double sum;
    private LinkedList<Double> list;
    private int size;
    double average;

    public MovingAverage(int size) {
        this.list = new LinkedList<>();
        this.size = size;
    }

    public double add(double val) {
        sum += val;
        list.offer(val);
        if (list.size() <= size) {
            return sum / size;
        }
        sum -= list.poll();
        return average = sum / size;
    }

    public double getAverage() {
        return average;
    }

}
