/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.datetime;

import java.util.Map;
import java.util.TreeMap;

public class Month {

	public static Month JANUARY = new Month(1, "January", "Jan");
	public static Month FEBRUARY = new Month(2, "February", "Feb");
	public static Month MARCH = new Month(3, "March", "Mar");
	public static Month APRIL = new Month(4, "April", "Apr");
	public static Month MAY = new Month(5, "May", "May");
	public static Month JUNE = new Month(6, "June", "Jun");
	public static Month JULY = new Month(7, "July", "Jul");
	public static Month AUGUST = new Month(8, "August", "Aug");
	public static Month SEPTEMBER = new Month(9, "Septeber", "Sep");
	public static Month OCTOBER = new Month(10, "October", "Oct");
	public static Month NOVEMBER = new Month(11, "November", "Nov");
	public static Month DECEMBER = new Month(12, "December", "Dec");

	private final int number;
	private final String longName;
	private final String shortName;

	private static final Month[] months = new Month[] { JANUARY, FEBRUARY,
			MARCH, APRIL, MAY, JUNE, JULY, AUGUST, SEPTEMBER, OCTOBER,
			NOVEMBER, DECEMBER };

	private static final Map<String, Month> shortNameIndex = new TreeMap<String, Month>(
			String.CASE_INSENSITIVE_ORDER);

	static {
		index(JANUARY);
		index(FEBRUARY);
		index(MARCH);
		index(APRIL);
		index(MAY);
		index(JUNE);
		index(JULY);
		index(AUGUST);
		index(SEPTEMBER);
		index(OCTOBER);
		index(NOVEMBER);
		index(DECEMBER);
	}

	public static Month month(String name) {
		return shortNameIndex.get(name);
	}

	public static Month month(int number) {
		return months[number - 1];
	}

	private Month(int number, String longName, String shortName) {
		this.number = number;
		this.longName = longName;
		this.shortName = shortName;
	}

	public int number() {
		return number;
	}

	public String longName() {
		return longName;
	}

	public String shortName() {
		return shortName;
	}

	private static void index(Month month) {
		shortNameIndex.put(month.shortName(), month);
	}
}