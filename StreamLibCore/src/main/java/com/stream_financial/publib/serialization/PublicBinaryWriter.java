/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.publib.serialization;

import com.stream_financial.core.data.Blob;
import com.stream_financial.core.data.DataType;
import com.stream_financial.core.datetime.DateTime;
import com.stream_financial.core.precision.Decimal;
import com.stream_financial.core.serialization.Endian;
import com.stream_financial.core.serialization.StreamWriter;

import java.util.Map;

public interface PublicBinaryWriter<T extends StreamWriter>
{
	T streamWriter();

	Endian endian();

	PublicBinaryWriter write(boolean value);

	PublicBinaryWriter write(byte value);

	PublicBinaryWriter write(short value);

	PublicBinaryWriter write(int value);

	PublicBinaryWriter write(long value);

	PublicBinaryWriter write(double value);

	PublicBinaryWriter write(float value);

	PublicBinaryWriter write(Blob value);

	PublicBinaryWriter write(Decimal value);

	PublicBinaryWriter write(String value);

	PublicBinaryWriter write(DateTime value);

	PublicBinaryWriter writeVectorFlex(double[] value, int maxSize);

	PublicBinaryWriter writeVectorFlex(float[] value, int maxSize);

    PublicBinaryWriter writeVectorFixed(double[] value, int size);

	default PublicBinaryWriter write(Object value) {
		return write(value, false);
	}

	default PublicBinaryWriter write(Object value, boolean writeDataType) {
		return write(value, writeDataType, null);
	}

	PublicBinaryWriter write(Object value, boolean writeDataType, DataType type);

	PublicBinaryWriter write(Map<String, Object> map);

	PublicBinaryWriter write(byte[] raw, int offset, int bytes);

	void commit();
}