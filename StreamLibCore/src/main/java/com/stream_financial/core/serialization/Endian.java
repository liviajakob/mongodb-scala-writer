/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.serialization;

import com.stream_financial.core.StreamException;

public enum Endian {

	Little(0),
	Big(1);

	private final int value;

	Endian(int value) {
		this.value = value;
	}

	public int value() {
		return value;
	}

	public static Endian Java = Endian.Big;

	public static Endian valueOf(int value) {
		switch (value) {
		case 0:
			return Little;
		case 1:
			return Big;
		default:
			throw new StreamException("Invalid endian value '%d'.", value);
		}
	}
}