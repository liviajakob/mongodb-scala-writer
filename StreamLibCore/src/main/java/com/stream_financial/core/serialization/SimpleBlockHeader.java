/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.serialization;

public class SimpleBlockHeader {

	private final byte[] data;
	private final int offset;

	private SimpleBlockHeader(byte[] data, int offset) {
		this.data = data;
		this.offset = offset;
	}

	public static SimpleBlockHeader wrap(byte[] data, int offset) {
		return new SimpleBlockHeader(data, offset);
	}

	public static int size() {
		return 16;
	}

	public Endian endian() {
		return Endian.valueOf(data[offset]);
	}

	public void setEndian(Endian endian) {
		data[offset] = (byte) endian.value();
	}

	public int dataSize() {
		return (int) Primitives.readLong(data, offset + 8, endian());
	}

	public void setDataSize(int dataSize) {
		Primitives.writeLong(data, offset + 8, dataSize, endian());
	}
}