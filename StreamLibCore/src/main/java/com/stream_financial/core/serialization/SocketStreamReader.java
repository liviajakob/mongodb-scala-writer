/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.serialization;

import com.stream_financial.core.StreamException;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class SocketStreamReader implements StreamReader, AutoCloseable
{
    private final MemoryBuffer buffer;
    private Endian endian;
    private final InputStream stream;

	public SocketStreamReader(int size, Socket socket)
	{
        this.buffer = new MemoryBuffer(0, size);
		this.stream = stream(socket);
	}

    @Override
    public Endian endian() {
        return endian;
    }

    @Override
    public void read(byte[] raw, int offset, int bytes) {

        if (bytes <= buffer.size()) {
            buffer.read(raw, offset, bytes);
        } else {

            // First empty the memory buffer.
            int remainingBytes = bytes;
            int size = buffer.size();
            if (size <= bytes && size > 0) {
                buffer.read(raw, offset, buffer.size());
                remainingBytes -= size;
            }

            // There is at least one whole block left to read,
            // so get block by block straight into raw.
            while (remainingBytes > buffer.length()) {
                byte[] headerBytes = new byte[SimpleBlockHeader.size()];
                int readCount = rawRead(headerBytes, 0, headerBytes.length);
                if (readCount < SimpleBlockHeader.size()) {
                    throw new StreamException(
                            "Cannot read enough bytes to get the header.");
                }
                SimpleBlockHeader header = SimpleBlockHeader.wrap(headerBytes,
                        0);
                if (endian == null) {
                    endian = header.endian();
                }
                int readRequired = header.dataSize();

                readCount = rawRead(raw, offset + bytes - remainingBytes,
                        readRequired);
                if (readCount < readRequired) {
                    throw new StreamException("Read is incomplete.");
                }
                remainingBytes -= readCount;
            }

            // Get another block and cache it in the memory buffer. Needs to
            // be at least remaining bytes. Return the part that is needed.
            if (remainingBytes > 0) {
                buffer.moveToFront();

                byte[] headerBytes = new byte[SimpleBlockHeader.size()];
                int readCount = rawRead(headerBytes, 0, headerBytes.length);
                SimpleBlockHeader header = SimpleBlockHeader.wrap(headerBytes,
                        0);
                if (endian == null) {
                    endian = header.endian();
                }
                int readRequired = header.dataSize();

                if (readCount < SimpleBlockHeader.size()) {
                    throw new StreamException(
                            "Cannot read enough bytes to get the header.");
                }

                if (buffer.size() > 0) {
                    throw new StreamException("Memory buffer should be empty.");
                }

                // Write into buffer.
                readCount = rawRead(buffer.buffer(), buffer.writeBlock(),
                        readRequired);

                // Sync the buffer indices.
                buffer.incrementWrite(readCount);
                if (readCount < remainingBytes) {
                    throw new StreamException(
                            "Not enough bytes to complete the request.");
                }

                // Read just what is required to complete the request.
                buffer.read(raw, offset + bytes - remainingBytes,
                        remainingBytes);
            }
        }
    }

    @Override
    public void skip(int bytes)
    {
        byte[] tmp = new byte[bytes];
        this.read(tmp, 0, tmp.length);
    }

    @Override
    public void reset()
    {
    }

	@Override
	public void close() {
		try {
			stream.close();
		} catch (IOException exception) {
			throw new StreamException(exception);
		}
	}

    private int rawRead(byte[] data, int offset, int size)
    {
        try {
            int index = 0;
            while (index < size) {
                int result = stream.read(data, index, size - index);
                if (result == -1) {
                    throw new StreamException(
                            "Failed to read beyond end of stream.");
                }
                index += result;
            }
            if (index != size) {
                throw new StreamException(
                        "Failed to read exact amount from stream.");
            }
            return index;
        } catch (IOException exception) {
            throw new StreamException(exception);
        }
    }

    private InputStream stream(Socket socket) {
        try {
            return socket.getInputStream();
        } catch (IOException exception) {
            throw new StreamException(exception);
        }
    }
}