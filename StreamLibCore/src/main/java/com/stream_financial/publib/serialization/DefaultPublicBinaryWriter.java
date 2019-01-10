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
import com.stream_financial.core.serialization.StreamWriter;

import java.util.Map;
import java.util.Map.Entry;

public class DefaultPublicBinaryWriter<T extends StreamWriter> implements PublicBinaryWriter<T> {
	private final byte[] temp = new byte[8];
	private final boolean serializeReferences;
	private final T streamWriter;

	public DefaultPublicBinaryWriter(T streamWriter, boolean serializeReferences) {
		this.streamWriter = streamWriter;
		this.serializeReferences = serializeReferences;
	}

	public void setContext(Object context) {
		this.streamWriter.setContext(context);
	}

	public void commit() {
		this.streamWriter.commit();
	}

	@Override
	public T streamWriter() {
		return this.streamWriter;
	}

	@Override
	public Endian endian() {
		return this.streamWriter.endian();
	}

	@Override
	public PublicBinaryWriter write(boolean value) {
		//Log.debug("Boolean:", value);
		temp[0] = value ? (byte) 1 : (byte) 0;
		streamWriter.write(temp, 0, 1);
		return this;
	}

	@Override
	public PublicBinaryWriter write(byte value) {
		//Log.debug("Byte:", value);
		temp[0] = value;
		streamWriter.write(temp, 0, 1);
		return this;
	}

	@Override
	public PublicBinaryWriter write(short value) {
		//Log.debug("Short:", value);
		Primitives.writeShort(temp, 0, value, endian());
		streamWriter.write(temp, 0, 2);
		return this;
	}

	@Override
	public PublicBinaryWriter write(int value) {
		//Log.debug("Int32:", value);
		Primitives.writeInt(temp, 0, value, endian());
		streamWriter.write(temp, 0, 4);
		return this;
	}

	@Override
	public PublicBinaryWriter write(long value) {
		//Log.debug("Int64:", value);
		Primitives.writeLong(temp, 0, value, endian());
		streamWriter.write(temp, 0, 8);
		return this;
	}

	@Override
	public PublicBinaryWriter write(double value) {
		//Log.debug("double:", value);
		Primitives.writeDouble(temp, 0, value, endian());
		streamWriter.write(temp, 0, 8);
		return this;
	}

	@Override
	public PublicBinaryWriter write(float value) {
		Primitives.writeFloat(temp, 0, value, endian());
		streamWriter.write(temp, 0, 4);
		return this;
	}

	@Override
	public PublicBinaryWriter write(Blob value) {
		if (value == null || value.isNull()) {
            write((long) 0);
            write(0);
        } else {
            write(value.uncompressedLength());
            write(value.compressedContent().length);
            write(value.compressedContent(), 0, value.compressedContent().length);
        }
		return this;
	}

	@Override
	public PublicBinaryWriter write(Decimal value)
	{
		write(value.scale());
		write(value.sign());
		write(value.high());
		write(value.mid());
		write(value.low());
		return this;
	}

	@Override
	public PublicBinaryWriter write(String value)
    {
		//Log.debug("String:", value);
		// NULL string?
		if (value == null) {
			write(-1);
			return this;
		}

		// Write.
		int size = value.length();
		write(size);
		if (size == 0) {
			return this;
		}

		byte[] raw = new byte[size];
		for (int i = 0; i < size; i++) {
			raw[i] = (byte) value.charAt(i);
		}
		write(raw, 0, size);
		return this;
	}

	@Override
	public PublicBinaryWriter write(DateTime value)
    {
		//Log.debug("DateTime:", value);
		write(value.ticks());
		return this;
	}

	public PublicBinaryWriter writeVectorFlex(double[] value, int maxSize)
	{
        if (value == null) {
            throw new StreamException("Invalid null value received for double vector");
        }

        if (value.length > maxSize) {
            throw new StreamException("Incorrect vector value; at most " + maxSize + " elements expected, but "
                    + value.length + " received");
        }

		write(value.length);
		for (double v : value) {
			write(v);
		}
		return this;
	}


	public PublicBinaryWriter writeVectorFlex(float[] value, int maxSize)
	{
		if (value == null) {
			throw new StreamException("Invalid null value received for float vector");
		}

		if (value.length > maxSize) {
			throw new StreamException("Incorrect vector value; at most " + maxSize + " elements expected, but "
					+ value.length + " received");
		}

		write(value.length);
		for (float v : value) {
			write(v);
		}
		return this;
	}

	public PublicBinaryWriter writeVectorFixed(double[] value, int size)
	{
	    if (value == null) {
	        throw new StreamException("Invalid null value received for double vector");
        }

	    if (value.length != size) {
	        throw new StreamException("Incorrect vector value; " + size + " elements expected, but "
                    + value.length + " received");
        }

		for (double v : value) {
			write(v);
		}
		return this;
	}

	@Override
	public PublicBinaryWriter write(Object value, boolean writeDataType, DataType type)
    {
        if (type == null || type == DataType.Variant || value == null) {
            type = DataType.of(value);
        }

		if (writeDataType) {
			write((byte) type.value());
		}
		switch (type) {
			case None:
				break;
			case Integer:
				write((int) value);
				break;
			case Double:
				write((double) value);
				break;
			case String:
				write((String) value);
				break;
			case DateTime:
				write((DateTime) value);
				break;
			case Boolean:
				write((boolean) value);
				break;
			case Blob:
				write((Blob) value);
				break;
			case Decimal:
				write((Decimal) value);
				break;
			case Long:
				write((long) value);
				break;
			/*case Variant:
                DataTypePub varType = DataType.of(value);
                writeValue(varType, value, out);
                break;*/
			case VectorDouble512:
				writeVectorFixed((double[]) value, 512);
				break;
			case VectorDouble513:
				writeVectorFixed((double[]) value, 513);
				break;
			case VectorDouble1024:
				writeVectorFixed((double[]) value, 1024);
				break;
			case VectorDoubleFlex265:
				writeVectorFlex((double[]) value, 265);
				break;
			case VectorDoubleFlex525:
				writeVectorFlex((double[]) value, 525);
				break;
			case VectorDoubleFlex800:
				writeVectorFlex((double[]) value, 800);
				break;
            case VectorFloatFlex32767:
                writeVectorFlex((float[]) value, 32767);
                break;
			default:
				throw new StreamException("Failed to writeValue of type %s (value type:  %s) to stream.",
                        type, value == null ? null : value.getClass());
		}
		return this;
	}

	@Override
	public PublicBinaryWriter write(Map<String, Object> map) {
		// NULL map?
		if (map == null) {
			write(0);
			return this;
		}

		// Write values.
		write(map.size());
		for (Entry<String, Object> entry : map.entrySet()) {
			write(entry.getKey());
			write(entry.getValue(), true);
		}
		return this;
	}

	@Override
	public PublicBinaryWriter write(byte[] raw, int offset, int bytes) {
		streamWriter.write(raw, offset, bytes);
		return this;
	}
}