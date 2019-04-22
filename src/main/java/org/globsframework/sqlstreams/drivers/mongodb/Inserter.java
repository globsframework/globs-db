package org.globsframework.sqlstreams.drivers.mongodb;

public class Inserter<T> {
    private int i = 0;
    private T[] values;

    public Inserter(T[] values) {
        this.values = values;
    }

    public void add(T t){
        values[i++] = t;
    }
}
