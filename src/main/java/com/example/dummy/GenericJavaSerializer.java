package com.example.dummy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public class GenericJavaSerializer<T extends Serializable> extends Serializer<T> implements Serializable {
    private transient JavaSerializer serializer;

    public GenericJavaSerializer() {
        serializer = new JavaSerializer();
    }

    @Override
    public void write(Kryo kryo, Output output, T t) {
        serializer.write(kryo, output, t);
    }

    @Override
    public T read(Kryo kryo, Input input, Class<T> type) {
        @SuppressWarnings("unchecked")
        T t = (T) serializer.read(kryo, input, type);

        return t;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        serializer = new JavaSerializer();
    }
}
