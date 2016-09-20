package com.example.dummy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.TreeMultimap;
import junit.framework.TestCase;

public class TreeMultimapSerializerTest extends TestCase {
    public static void testKryoJavaSerialization() {
        TreeMultimap<Integer, String> treeMultimap = TreeMultimap.create();
        treeMultimap.put(1, "Hello");
        treeMultimap.put(2, "World");
        treeMultimap.put(1, "Bye");

        byte[] buffer = new byte[8192];
        Kryo kryo = new Kryo();
        kryo.addDefaultSerializer(TreeMultimap.class, new GenericJavaSerializer<TreeMultimap>());

        // Kryo.writeObject
        try (Output output = new Output(buffer)) {
            kryo.writeObject(output, treeMultimap);
        }

        try (Input input = new Input(buffer)) {
            @SuppressWarnings("unchecked")
            TreeMultimap<Integer, String> newTreeMultimap = kryo.readObject(input, TreeMultimap.class);
            assert newTreeMultimap.get(1).containsAll(ImmutableList.of("Hello", "Bye"));
            assert newTreeMultimap.get(2).contains("World");
        }

        // Kryo.writeClassAndObject
        buffer = new byte[8192];
        try (Output output = new Output(buffer)) {
            kryo.writeClassAndObject(output, treeMultimap);
        }

        try (Input input = new Input(buffer)) {
            Object obj = kryo.readClassAndObject(input);
            assert obj instanceof TreeMultimap;

            @SuppressWarnings("unchecked")
            TreeMultimap<Integer, String> newTreeMultimap = (TreeMultimap<Integer, String>) obj;

            assert newTreeMultimap.get(1).containsAll(ImmutableList.of("Hello", "Bye"));
            assert newTreeMultimap.get(2).contains("World");
        }

    }

    public static void testKryoCustomSerialization() {
        TreeMultimap<Integer, String> treeMultimap = TreeMultimap.create();
        treeMultimap.put(1, "Hello");
        treeMultimap.put(2, "World");
        treeMultimap.put(1, "Bye");

        byte[] buffer = new byte[8192];
        Kryo kryo = new Kryo();
        kryo.addDefaultSerializer(TreeMultimap.class, new TreeMultimapSerializer<>(String.class));

        // Kryo.writeObject
        try (Output output = new Output(buffer)) {
            kryo.writeObject(output, treeMultimap);
        }

        try (Input input = new Input(buffer)) {
            @SuppressWarnings("unchecked")
            TreeMultimap<Integer, String> newTreeMultimap = kryo.readObject(input, TreeMultimap.class);
            assert newTreeMultimap.get(1).containsAll(ImmutableList.of("Hello", "Bye"));
            assert newTreeMultimap.get(2).contains("World");
        }

        // Kryo.writeClassAndObject
        buffer = new byte[8192];
        try (Output output = new Output(buffer)) {
            kryo.writeClassAndObject(output, treeMultimap);
        }

        try (Input input = new Input(buffer)) {
            Object obj = kryo.readClassAndObject(input);
            assert obj instanceof TreeMultimap;

            @SuppressWarnings("unchecked")
            TreeMultimap<Integer, String> newTreeMultimap = (TreeMultimap<Integer, String>) obj;

            assert newTreeMultimap.get(1).containsAll(ImmutableList.of("Hello", "Bye"));
            assert newTreeMultimap.get(2).contains("World");
        }

    }
}
