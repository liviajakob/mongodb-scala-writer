/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.publib.serialization;

import com.stream_financial.core.data.Blob;
import com.stream_financial.core.datetime.DateTime;
import com.stream_financial.core.precision.Decimal;
import com.stream_financial.core.serialization.StreamReader;

import java.util.Map;

public interface PublicBinaryReader<T extends StreamReader>
{
	T streamReader();

	boolean readBoolean();

	byte readByte();

	short readShort();

	int readInt();

	long readLong();

	double readDouble();

	float readFloat();

	Blob readBlob();

	Decimal readDecimal();

	String readString();

	DateTime readDateTime();

	Object readValue();

	Map<String, Object> readMap();

	void read(byte[] raw, int offset, int bytes);

	void skip(int bytes);

	void clear();
}