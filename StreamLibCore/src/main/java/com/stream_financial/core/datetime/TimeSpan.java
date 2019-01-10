/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.datetime;

public class TimeSpan {

	public static final long TICKS_PER_MILLISECOND = 10000;
	public static final long TICKS_PER_SECOND = TICKS_PER_MILLISECOND * 1000;
	public static final long TICKS_PER_MINUTE = TICKS_PER_SECOND * 60;

	long ticks;

	public TimeSpan(long ticks) {
		this.ticks = ticks;
	}

	public long totalMilliseconds()
	{
		return ticks / TICKS_PER_MILLISECOND;
	}

	public long ticks()
	{
		return this.ticks;
	}

	public TimeSpan(int days, int hours, int minutes, int seconds, int milliseconds)
	{
		long totalMilliSeconds = ((long) days * 3600 * 24 + hours * 3600
				+ minutes * 60 + seconds)
				* 1000 + milliseconds;
		ticks = totalMilliSeconds * TICKS_PER_MILLISECOND;
	}

	public static long timeToTicks(int hour, int minute, int second)
	{
		long totalSeconds = (long) hour * 3600 + (long) minute * 60 + (long) second;
		return totalSeconds * TICKS_PER_SECOND;
	}
}