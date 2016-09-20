package com.example.dummy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.TreeMultimap;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

public class TreeMultimapSerializer<T extends Comparable>
        extends Serializer<TreeMultimap<Integer, T>> implements Serializable {

    Class<T> clazz;

    public TreeMultimapSerializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void write(Kryo kryo, Output output, TreeMultimap<Integer, T> treeMultimap) {
        try {
            output.writeInt(treeMultimap.asMap().size());
            for (Map.Entry<Integer, Collection<T>> kv : treeMultimap.asMap().entrySet()) {
                output.writeInt(kv.getKey());  // TODO: optimizePositive?
                output.writeInt(kv.getValue().size());
                for (T x : kv.getValue()) {
                    kryo.writeObject(output, x);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public TreeMultimap<Integer, T> read(Kryo kryo, Input input, Class<TreeMultimap<Integer, T>> clazz) {
        // For simplicity, just uses the default (Ordering.natural()) Comparator
        TreeMultimap<Integer, T> treeMultimap = TreeMultimap.create();

        try {
            int nKeys = input.readInt();
            for (int i = 0; i < nKeys; i++) {
                Integer k = input.readInt();
                Collection<T> v = treeMultimap.get(k);
                int nValuesInThisKey = input.readInt();
                for (int j = 0; j < nValuesInThisKey; j++) {
                    v.add((T) kryo.readObject(input, this.clazz));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return treeMultimap;
    }
}
