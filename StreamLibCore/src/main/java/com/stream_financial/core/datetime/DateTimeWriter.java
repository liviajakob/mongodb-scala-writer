/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.datetime;

import com.stream_financial.core.StreamException;

public class DateTimeWriter {

	private final StringBuilder text = new StringBuilder();

	public void write(DateTime dateTime, String format) {

		// Default.
		if (format == null) {
			write(4, dateTime.year());
			text.append('-');
			write(2, dateTime.month());
			text.append('-');
			write(2, dateTime.day());
			if (dateTime.hour() != 0 || dateTime.minute() != 0
					|| dateTime.second() != 0 || dateTime.millisecond() != 0) {
				text.append(' ');
				write(2, dateTime.hour());
				text.append(':');
				write(2, dateTime.minute());
				text.append(':');
				write(2, dateTime.second());
				if (dateTime.millisecond() != 0) {
					text.append('.');
					write(3, dateTime.millisecond());
				}
			}
			return;
		}

		// YYYY-MM-dd
		if (format.equals(DateTimeFormat.SHORT_FORMAT)) {
			write(4, dateTime.year());
			text.append('-');
			write(2, dateTime.month());
			text.append('-');
			write(2, dateTime.day());
			return;
		}

		// YYYY-MM-dd HH:mm:ss.SSS
		if (format == DateTimeFormat.LONG_FORMAT) {
			write(4, dateTime.year());
			text.append('-');
			write(2, dateTime.month());
			text.append('-');
			write(2, dateTime.day());
			text.append(' ');
			write(2, dateTime.hour());
			text.append(':');
			write(2, dateTime.minute());
			text.append(':');
			write(2, dateTime.second());
			text.append('.');
			write(3, dateTime.millisecond());
			return;
		}

		// dd-MMM-yy
		if (format == DateTimeFormat.SHORT_MONTH_FORMAT) {
			write(2, dateTime.day());
			text.append('-');
			text.append(Month.month(dateTime.month()).shortName());
			text.append('-');
			write(2, dateTime.year() - 2000);
			return;
		}

		// dd-MMM-yyyy
		if (format == DateTimeFormat.SHORT_MONTH_FORMAT_FULL_YEAR) {
			write(2, dateTime.day());
			text.append('-');
			text.append(Month.month(dateTime.month()).shortName());
			text.append('-');
			write(4, dateTime.year());
			return;
		}

		// Invalid.
		throw new StreamException("Invalid date time format: " + format);
	}

	public String toString() {
		return text.toString();
	}

	private void write(int size, int value) {
		String item = Integer.toString(value);
		int padding = size - item.length();
		for (int i = 0; i < padding; i++) {
			text.append('0');
		}
		text.append(item);
	}
}