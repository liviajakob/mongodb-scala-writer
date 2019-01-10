/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.serialization;

public class MemoryBuffer {

	private int start;
	private int end;
	private final byte[] buffer;
	private final int offset;
	private final int length;

	public MemoryBuffer(int offset, int size) {
		this.offset = offset;
		this.length = size;
		this.buffer = new byte[size + offset];
	}

	public byte[] buffer() {
		return buffer;
	}

	public int length() {
		return length;
	}

	public int size() {
		return end - start;
	}

	public int space() {
		return length - end;
	}

	public int totalSpace() {
		return length - size();
	}

	public int raw() {
		return 0;
	}

	public int start() {
		return offset;
	}

	public int writeBlock() {
		return offset + end;
	}

	public int readBlock() {
		return offset + start;
	}

	public void incrementWrite(int size) {
		end += size;
	}

	public void incrementRead(int size) {
		start += size;
	}

	public void moveToFront() {
		if (start != 0) {
			System.arraycopy(buffer, readBlock(), buffer, start(), size());
			end = size();
			start = 0;
		}
	}

	public boolean read(byte[] bytes, int offset, int size) {
		if (size > size()) {
			return false;
		} else {
			System.arraycopy(buffer, readBlock(), bytes, offset, size);
			start += size;
			return true;
		}
	}

	public boolean write(byte[] bytes, int offset, int size) {

		// Is there enough at the right of the buffer?
		if (size > space()) {

			// Is there room if one moves stuff around?
			if (size <= totalSpace()) {
				moveToFront();
			} else {
				// Cannot write.
				return false;
			}
		}

		// Do the write.
		System.arraycopy(bytes, offset, buffer, writeBlock(), size);
		end += size;
		return true;
	}
}