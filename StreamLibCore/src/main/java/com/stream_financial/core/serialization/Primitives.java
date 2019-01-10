/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.serialization;

public class Primitives {

	private Primitives() {
	}

	public static short readShort(byte[] data, int offset, Endian endian) {

		if (endian == Endian.Little) {
			return (short) ((data[offset++] & 0xFF) | (data[offset] & 0xFF) << 8);
		} else {
			return (short) ((data[offset++] & 0xFF) << 8 | (data[offset] & 0xFF));
		}
	}

	public static void writeShort(byte[] data, int offset, short value,
			Endian endian) {

		if (endian == Endian.Little) {
			data[offset++] = (byte) (value);
			data[offset] = (byte) (value >> 8);
		} else {
			data[offset++] = (byte) (value >> 8);
			data[offset] = (byte) (value);
		}
	}

	public static int readInt(byte[] data, int offset, Endian endian) {

		if (endian == Endian.Little) {
			return (data[offset++] & 0xFF) | (data[offset++] & 0xFF) << 8
					| (data[offset++] & 0xFF) << 16
					| (data[offset] & 0xFF) << 24;
		} else {
			return (data[offset++] & 0xFF) << 24
					| (data[offset++] & 0xFF) << 16
					| (data[offset++] & 0xFF) << 8 | (data[offset] & 0xFF);
		}
	}

	public static void writeInt(byte[] data, int offset, int value, Endian endian)
	{
		if (endian == Endian.Little) {
			data[offset++] = (byte) (value);
			data[offset++] = (byte) (value >> 8);
			data[offset++] = (byte) (value >> 16);
			data[offset] = (byte) (value >> 24);
		} else {
			data[offset++] = (byte) (value >> 24);
			data[offset++] = (byte) (value >> 16);
			data[offset++] = (byte) (value >> 8);
			data[offset] = (byte) (value);
		}
	}

	public static long readLong(byte[] data, int offset, Endian endian) {

		if (endian == Endian.Little) {
			return (data[offset++] & 0xFFL) | (data[offset++] & 0xFFL) << 8
					| (data[offset++] & 0xFFL) << 16
					| (data[offset++] & 0xFFL) << 24
					| (data[offset++] & 0xFFL) << 32
					| (data[offset++] & 0xFFL) << 40
					| (data[offset++] & 0xFFL) << 48
					| (data[offset] & 0xFFL) << 56;
		} else {
			return (data[offset++] & 0xFFL) << 56
					| (data[offset++] & 0xFFL) << 48
					| (data[offset++] & 0xFFL) << 40
					| (data[offset++] & 0xFFL) << 32
					| (data[offset++] & 0xFFL) << 24
					| (data[offset++] & 0xFFL) << 16
					| (data[offset++] & 0xFFL) << 8 | (data[offset] & 0xFFL);
		}
	}

	public static void writeLong(byte[] data, int offset, long value,
			Endian endian) {

		if (endian == Endian.Little) {
			data[offset++] = (byte) (value);
			data[offset++] = (byte) (value >> 8);
			data[offset++] = (byte) (value >> 16);
			data[offset++] = (byte) (value >> 24);
			data[offset++] = (byte) (value >> 32);
			data[offset++] = (byte) (value >> 40);
			data[offset++] = (byte) (value >> 48);
			data[offset] = (byte) (value >> 56);
		} else {
			data[offset++] = (byte) (value >> 56);
			data[offset++] = (byte) (value >> 48);
			data[offset++] = (byte) (value >> 40);
			data[offset++] = (byte) (value >> 32);
			data[offset++] = (byte) (value >> 24);
			data[offset++] = (byte) (value >> 16);
			data[offset++] = (byte) (value >> 8);
			data[offset] = (byte) (value);
		}
	}

	public static double readDouble(byte[] data, int offset, Endian endian) {
		return Double.longBitsToDouble(readLong(data, offset, endian));
	}

	public static float readFloat(byte[] data, int offset, Endian endian) {
		return Float.intBitsToFloat(readInt(data, offset, endian));
	}

	public static void writeDouble(byte[] data, int offset, double value, Endian endian) {
		writeLong(data, offset, Double.doubleToLongBits(value), endian);
	}

	public static void writeFloat(byte[] data, int offset, float value, Endian endian) {
		writeInt(data, offset, Float.floatToIntBits(value), endian);
	}
}