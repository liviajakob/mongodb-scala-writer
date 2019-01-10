/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.datetime;

public enum DayOfWeek {

	Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday;

	private static final DayOfWeek[] VALUES = { Sunday, Monday, Tuesday,
			Wednesday, Thursday, Friday, Saturday };

	public static DayOfWeek dayOfWeek(int ordinal) {
		return VALUES[ordinal];
	}
}