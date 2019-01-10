package com.stream_financial.core.data;

import com.stream_financial.core.StreamException;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class Blob implements Comparable<Blob> {
    private byte[] compressedContent;
    private long uncompressedLength;

    public Blob(byte[] content, long uncompressedLength) {
        this.compressedContent = content;
        this.uncompressedLength = uncompressedLength;
    }

    public Blob(String uncompressedContent) {
        compressedContent = deflate(uncompressedContent);
    }

    private byte[] deflate(String uncompressedContent) {
        byte[] input = uncompressedContent.getBytes(StandardCharsets.UTF_8);
        uncompressedLength = input.length;

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        Deflater compressor = new Deflater();
        compressor.setInput(input);
        compressor.finish();

        byte[] buffer = new byte[4096];
        while (!compressor.finished()) {
            int n = compressor.deflate(buffer);
            byteStream.write(buffer, 0, n);
        }
        compressor.end();

        return byteStream.toByteArray();
    }

    @Override
    public boolean equals(Object nextObject) {
        if (!(nextObject instanceof Blob)) {
            return false;
        }
        return (Arrays.equals(compressedContent, ((Blob) nextObject).compressedContent()));
    }

    @Override
    public int hashCode() {
        return Blob.hashCode(this);
    }

    public static int hashCode(Blob value) {
        return Arrays.hashCode(value.compressedContent);
    }

    @Override
    public int compareTo(Blob nextBlob) {
        //return Arrays.compare(compressedContent, nextBlob.compressedContent()); // Only available in Java 9+
        return compareByteArray(compressedContent, nextBlob.compressedContent);
    }

    private int compareByteArray(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }

    public boolean isNull() {
        return (compressedContent.length == 0);
    }

    public long uncompressedLength() {
        return uncompressedLength;
    }

    public byte[] compressedContent() {
        return compressedContent;
    }

    public String inflate() {
        try {
            // Decompress the bytes
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            Inflater decompressor = new Inflater();
            decompressor.setInput(compressedContent);
            byte[] buffer = new byte[4096];

            while (!decompressor.finished()) {
                int n = decompressor.inflate(buffer);
                byteStream.write(buffer, 0, n);
            }
            decompressor.end();

            byte[] result = byteStream.toByteArray();

            // Decode the bytes into a String
            String outputString = new String(result, StandardCharsets.UTF_8);
            return outputString;
        } catch (java.util.zip.DataFormatException e) {
            throw new StreamException("Data format error: " + e.getMessage());
        }
    }
}