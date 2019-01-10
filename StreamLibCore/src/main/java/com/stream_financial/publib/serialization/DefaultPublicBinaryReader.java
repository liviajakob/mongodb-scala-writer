/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.publib.serialization;

import com.stream_financial.core.StreamException;
import com.stream_financial.core.data.Blob;
import com.stream_financial.core.data.DataType;
import com.stream_financial.core.datetime.DateTime;
import com.stream_financial.core.precision.Decimal;
import com.stream_financial.core.serialization.Endian;
import com.stream_financial.core.serialization.Primitives;
import com.stream_financial.core.serialization.StreamReader;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class DefaultPublicBinaryReader<T extends StreamReader> implements PublicBinaryReader<T>
{
    private final T streamReader;
    private final boolean serializeReferences;
    private final byte[] temp = new byte[16];

    public DefaultPublicBinaryReader(T streamReader, boolean serializeReferences)
    {
        this.streamReader = streamReader;
        this.serializeReferences = serializeReferences;
    }

    public Endian endian()
    {
        return this.streamReader.endian();
    }

    @Override
    public T streamReader()
    {
        return this.streamReader;
    }

    @Override
    public boolean readBoolean() {
        this.streamReader.read(temp, 0, 1);
        return temp[0] != 0;
    }

    @Override
    public byte readByte() {
        this.streamReader.read(temp, 0, 1);
        return temp[0];
    }

    @Override
    public short readShort() {
        this.streamReader.read(temp, 0, 2);
        return Primitives.readShort(temp, 0, this.endian());
    }

    @Override
    public int readInt() {
        this.streamReader.read(temp, 0, 4);
        return Primitives.readInt(temp, 0, this.endian());
    }

    @Override
    public long readLong() {
        this.streamReader.read(temp, 0, 8);
        return Primitives.readLong(temp, 0, this.endian());
    }

    @Override
    public double readDouble() {
        this.streamReader.read(temp, 0, 8);
        return Primitives.readDouble(temp, 0, this.endian());
    }

    @Override
    public float readFloat() {
        this.streamReader.read(temp, 0, 4);
        return Primitives.readFloat(temp, 0, this.endian());
    }

    @Override
    public Blob readBlob() {
        long uncompressedLength = readLong();
        int size = readInt();

        if (size == 0) {
            return null;
        } else {
            byte[] bytes = new byte[size];
            this.streamReader.read(bytes, 0, size);

            return new Blob(bytes, uncompressedLength);
        }
    }

    @Override
    public Decimal readDecimal() {
        byte scale = readByte();
        boolean isNegative = readBoolean();
        int high = readInt();
        int mid = readInt();
        int low = readInt();

        Decimal dec = new Decimal(low, mid, high, isNegative, scale);

        return dec;
    }

    @Override
    public String readString()
    {
        // NULL string?
        int size = readInt();
        if (size == -1) {
            return null;
        }

        // Empty string?
        if (size == 0) {
            return "";
        }

        // Read.
        byte[] raw = new byte[size];
        read(raw, 0, size);

        String outString;
        try {
            outString = new String(raw, "ISO-8859-1");
        } catch(UnsupportedEncodingException se) {
            throw new StreamException("You are not authorised to create this folder.");
        }

        return outString;
    }

    @Override
    public DateTime readDateTime() {
        return new DateTime(readLong());
    }

    @Override
    public Object readValue() {
        Byte typeByte = this.readByte();
        DataType type = DataType.valueOf((int) typeByte);
        if (type == null) {
            throw new StreamException("Unrecognised data type code: %d.", typeByte);
        }

        switch (type) {
            case None:
                return null;
            case Integer:
                return this.readInt();
            case Double:
                return this.readDouble();
            case String:
                return this.readString();
            case DateTime:
                return this.readDateTime();
            case Boolean:
                return this.readBoolean();
            case Blob:
                return this.readBlob();
            case Decimal:
                return this.readDecimal();
            case Long:
                return this.readLong();
            /*case Variant:
                return readValue();*/
            case VectorDouble512:
                return this.readVectorDoubleFixed(512);
            case VectorDouble513:
                return this.readVectorDoubleFixed(513);
            case VectorDouble1024:
                return this.readVectorDoubleFixed(1024);
            case VectorDoubleFlex265:
            case VectorDoubleFlex525:
            case VectorDoubleFlex800:
                return this.readVectorDoubleFlex();
            case VectorFloatFlex32767:
                return this.readVectorFloatFlex();
            default:
                throw new StreamException("Data type '%s' not supported.", type);
        }
    }

    private double[] readVectorDoubleFixed(int size) {
        double[] array = new double[size];
        for (int i = 0; i < size; i++) {
            array[i] = readDouble();
        }
        return array;
    }

    private double[] readVectorDoubleFlex() {
        int size = readInt();
        double[] array = new double[size];
        for (int i = 0; i < size; i++) {
            array[i] = readDouble();
        }
        return array;
    }

    private float[] readVectorFloatFlex() {
        int size = readInt();
        float[] array = new float[size];
        for (int i = 0; i < size; i++) {
            array[i] = readFloat();
        }
        return array;
    }

    @Override
    public Map<String, Object> readMap() {
        Map<String, Object> map = new HashMap<>();
        int size = readInt();
        for (int i = 0; i < size; i++) {
            map.put(readString(), readValue());
        }
        return map;
    }

    @Override
    public void read(byte[] raw, int offset, int bytes)
    {
        this.streamReader.read(raw, offset, bytes);
    }

    @Override
    public void skip(int bytes)
    {
        this.streamReader.skip(bytes);
    }

    @Override
    public void clear() {
        this.streamReader.reset();
    }

    @Override
    public String toString(){
        return String.format("%d  (internal reader: %d)", System.identityHashCode(this),
                System.identityHashCode(this.streamReader));
    }
}