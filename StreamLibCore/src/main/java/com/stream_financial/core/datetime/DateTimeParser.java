/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.datetime;

import com.stream_financial.core.StreamException;

import java.util.Map;
import java.util.TreeMap;

public class DateTimeParser {

	private static final Map<String, Integer> shortMonths = new TreeMap<String, Integer>(
			String.CASE_INSENSITIVE_ORDER);

	static {
		shortMonths.put("Jan", 1);
		shortMonths.put("Feb", 2);
		shortMonths.put("Mar", 3);
		shortMonths.put("Apr", 4);
		shortMonths.put("May", 5);
		shortMonths.put("Jun", 6);
		shortMonths.put("Jul", 7);
		shortMonths.put("Aug", 8);
		shortMonths.put("Sep", 9);
		shortMonths.put("Oct", 10);
		shortMonths.put("Nov", 11);
		shortMonths.put("Dec", 12);
	}

	private final String text;
	private final boolean isUSDate;
	private final int endIndex;
	private int index;
	private int year;
	private int month;
	private int day;
	private int minute;
	private int hour;
	private int second;
	private int millisecond;
	private boolean hasYear;
	private boolean hasMonth;
	private boolean hasDay;
	private boolean hasMinute;
	private boolean hasHour;
	private boolean hasSecond;
	private boolean hasMillisecond;

	public DateTimeParser(String text, boolean isUSDate) {
		this.text = text;
		this.isUSDate = isUSDate;
		this.endIndex = text != null ? text.length() - 1 : -1;
	}

	public DateTime read() {

		// NULL?
		if (endIndex < 0) {
			return null;
		}

		while (canRead()) {

			// 0 ... 9
			int start = index;
			if (digit(peek())) {
				index++;
				while (canRead() && digit(peek())) {
					index++;
				}
				int value = Integer.parseInt(text(start, index));
				if (!isUSDate) {
					if (!hasYear && !hasDay) {
						if (value <= 31) {
							day = value;
							hasDay = true;
						} else {
							year = value;
							hasYear = true;
						}
					} else if ((hasYear || hasDay) && !hasMonth) {
						month = value;
						hasMonth = true;
					} else if (!hasYear) {
						year = value;
						if (year < 100) {
							if (year >= 50) {
								year = 1900 + year;
							} else {
								year = 2000 + year;
							}
						}
						hasYear = true;
					} else if (!hasDay) {
						day = value;
						hasDay = true;
					} else if (!hasHour) {
						hour = value;
						hasHour = true;
					} else if (!hasMinute) {
						minute = value;
						hasMinute = true;
					} else if (!hasSecond) {
						second = value;
						hasSecond = true;
					} else if (!hasMillisecond) {
						millisecond = value;
						hasMillisecond = true;
					} else {
						throw formatException();
					}
					continue;
				}
			}

			// A ... Z
			if (letter(peek())) {
				index++;
				while (canRead() && letter(peek())) {
					index++;
				}
				if (hasMonth) {
					//throw formatException();
					String txt = text(start, index);
					if (txt.equalsIgnoreCase("AM")) {
						continue;
					} else if (txt.equalsIgnoreCase("PM")) {
						hour += 12;
						continue;
					} else {
						throw formatException();
					}
				} else {
					Integer value = shortMonths.get(text(start, index));
					if (value == null) {
						throw formatException();
					}
					month = value;
					hasMonth = true;
					continue;
				}
			}

			// Separator.
			char ch = peek();
			if (ch == '-' || ch == ':' || ch == '.' || ch == ' ' || ch == '/') {
				index++;
				continue;
			}

			// Invalid.
			throw formatException();
		}

		if (hasYear && hasMonth && hasDay)
		{
			return new DateTime(year, month, day, hour, minute, second, millisecond);
		}

		// Invalid.
		throw formatException();
	}

	private String text(int startIndex, int endIndexExclusive) {
		return text.substring(startIndex, endIndexExclusive);
	}

	private static boolean letter(char ch) {
		return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z');
	}

	private static boolean digit(char ch) {
		return ch >= '0' && ch <= '9';
	}

	private boolean canRead() {
		return index <= endIndex;
	}

	private char peek() {
		if (index > endIndex) {
			throw new StreamException("Failed to peek beyond end of stream.");
		}
		return text.charAt(index);
	}

	private StreamException formatException() {
		return new StreamException("Format not recognized.");
	}
}