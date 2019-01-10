/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.serialization;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import com.stream_financial.core.StreamException;

public class SocketStreamWriter implements StreamWriter, AutoCloseable
{
    private final MemoryBuffer buffer;
    private final Endian endian;
    private final OutputStream stream;

	public SocketStreamWriter(int size, Socket socket, Endian endian)
    {
        this.endian = endian;
        this.buffer = new MemoryBuffer(SimpleBlockHeader.size(), size);
		this.stream = stream(socket);
	}

    @Override
    public Endian endian() {
        return endian;
    }

    @Override
    public void setContext(Object context)
    {
        // Not needed...
    }

    @Override
    public void write(byte[] raw, int offset, int bytes)
    {
        // This has been written to the buffer and it fits.
        if (buffer.write(raw, offset, bytes)) {
            return;
        }

        // The buffer does not have enough space left.
        // First fill the memory buffer.
        int remainingBytes = bytes;
        int bytesToWrite = buffer.totalSpace();
        if (!buffer.write(raw, offset, bytesToWrite)) {
            throw new StreamException("Failed to write to buffer.");
        }
        commit();
        remainingBytes -= bytesToWrite;

        // Write and commit the chunks.
        while (remainingBytes > buffer.length()) {
            if (buffer.write(raw, offset + bytes - remainingBytes,
                    buffer.length())) {
                throw new StreamException("Failed to write to buffer.");
            }
            if (buffer.totalSpace() != 0) {
                throw new StreamException("Unexpected space in buffer.");
            }
            remainingBytes -= buffer.length();
            commit();
            if (buffer.size() != 0) {
                throw new StreamException("Unexpected buffer size.");
            }
        }

        // Put the remainder in the buffer.
        if (remainingBytes > 0
                && !buffer.write(raw, offset + bytes - remainingBytes,
                remainingBytes)) {
            throw new StreamException("Failed to write to buffer.");
        }
    }

    @Override
    public void commit()
    {
        // Offset and contents need to be continguous.
        buffer.moveToFront();
        int size = buffer.size();
        SimpleBlockHeader header = SimpleBlockHeader.wrap(buffer.buffer(),
                buffer.raw());
        header.setEndian(endian);
        header.setDataSize(size);

        // Write to the buffer including header.
        rawWrite(buffer.buffer(), buffer.raw(), size + SimpleBlockHeader.size());
        buffer.incrementRead(size);
    }

    @Override
    public void close() {
        commit();
        try {
            stream.flush();
            stream.close();
        } catch (IOException exception) {
            throw new StreamException(exception);
        }
    }

	private void rawWrite(byte[] data, int offset, int size) {
		try {
			stream.write(data, offset, size);
		} catch (IOException exception) {
			throw new StreamException(exception);
		}
	}

    private OutputStream stream(Socket socket) {
        try {
            return socket.getOutputStream();
        } catch (IOException exception) {
            throw new StreamException(exception);
        }
    }
}