/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.datetime;

import com.stream_financial.core.StreamException;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

public class DateTime implements Comparable<DateTime> {

	public static final long TICKS_PER_MILLISECOND = 10000;
	public static final long TICKS_PER_SECOND = TICKS_PER_MILLISECOND * 1000;
	public static final long TICKS_PER_MINUTE = TICKS_PER_SECOND * 60;
	public static final long TICKS_PER_HOUR = TICKS_PER_MINUTE * 60;
	public static final long TICKS_PER_DAY = TICKS_PER_HOUR * 24;

	private static final int MILLIS_PER_SECOND = 1000;
	private static final int MILLIS_PER_MINUTE = MILLIS_PER_SECOND * 60;
	private static final int MILLIS_PER_HOUR = MILLIS_PER_MINUTE * 60;
	private static final int MILLIS_PER_DAY = MILLIS_PER_HOUR * 24;

	private static final int DAYS_PER_YEAR = 365;
	private static final int DAYS_PER_4_YEARS = DAYS_PER_YEAR * 4 + 1;
	private static final int DAYS_PER_100_YEARS = DAYS_PER_4_YEARS * 25 - 1;
	private static final int DAYS_PER_400_YEARS = DAYS_PER_100_YEARS * 4 + 1;

	private static final long UNIX_EPOCH_TICKS = 621355968000000000L;

	private static final int[] DAYS_TO_MONTH_365 = { 0, 31, 59, 90, 120, 151,
			181, 212, 243, 273, 304, 334, 365 };
	private static final int[] DAYS_TO_MONTH_366 = { 0, 31, 60, 91, 121, 152,
			182, 213, 244, 274, 305, 335, 366 };

	private static final int DATE_PART_YEAR = 0;
	private static final int DATE_PART_DAY_OF_YEAR = 1;
	private static final int DATE_PART_MONTH = 2;
	private static final int DATE_PART_DAY = 3;

	private long ticks;

	public static DateTime MAX_VALUE = new DateTime(Long.MAX_VALUE);

	public DateTime() {
		ticks = currentTicks();
	}

	@SuppressWarnings("deprecation")
	public DateTime(Date date) {
		this(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
	}

	@SuppressWarnings("deprecation")
	public DateTime(Timestamp timestamp) {
		this(timestamp.getYear() + 1900, timestamp.getMonth() + 1, timestamp
				.getDate(), timestamp.getHours(), timestamp.getMinutes(),
				timestamp.getSeconds(), timestamp.getNanos() / 1000000);
	}

	public DateTime(long ticks)
	{
		this.ticks = ticks;
	}

	public DateTime(int year, int month, int day) {
		ticks = dateToTicks(year, month, day);
	}

	public DateTime(int year, int month, int day, int hour, int minute,
			int second) {
		ticks = dateToTicks(year, month, day)
				+ timeToTicks(hour, minute, second);
	}

	public DateTime(int year, int month, int day, int hour, int minute, int second, int millisecond) {
		ticks = dateToTicks(year, month, day)
				+ timeToTicks(hour, minute, second);

		if (millisecond < 0 || millisecond >= MILLIS_PER_SECOND) {
			throw new StreamException("Invalid milliseconds.");
		}
		ticks += millisecond * TICKS_PER_MILLISECOND;
	}

	public int year() {
		return datePart(ticks, DATE_PART_YEAR);
	}

	public int month() {
		return datePart(ticks, DATE_PART_MONTH);
	}

	public int day() {
		return datePart(ticks, DATE_PART_DAY);
	}

	public DayOfWeek dayOfWeek() {
		return DayOfWeek.dayOfWeek((int) ((ticks / TICKS_PER_DAY + 1) % 7));
	}

	public int dayOfYear() {
		return datePart(ticks, DATE_PART_DAY_OF_YEAR);
	}

	public int hour() {
		return (int) ((ticks / TICKS_PER_HOUR) % 24);
	}

	public int minute() {
		return (int) ((ticks / TICKS_PER_MINUTE) % 60);
	}

	public int second() {
		return (int) ((ticks / TICKS_PER_SECOND) % 60);
	}

	public int millisecond() {
		return (int) ((ticks / TICKS_PER_MILLISECOND) % 1000);
	}

	public long ticks() {
		return ticks;
	}

	public DateTime add(TimeSpan value) {
		return new DateTime(ticks + value.ticks);
	}

	public DateTime addYears(int value) {
		return addMonths(value * 12);
	}

	public DateTime addMonths(int months) {
		int y = datePart(ticks, DATE_PART_YEAR);
		int m = datePart(ticks, DATE_PART_MONTH);
		int d = datePart(ticks, DATE_PART_DAY);
		int i = m - 1 + months;
		if (i >= 0) {
			m = i % 12 + 1;
			y = y + i / 12;
		} else {
			m = 12 + (i + 1) % 12;
			y = y + (i - 11) / 12;
		}
		int days = daysInMonth(y, m);
		if (d > days) {
			d = days;
		}
		return new DateTime(dateToTicks(y, m, d) + ticks % TICKS_PER_DAY);
	}

	public DateTime addDays(long value) {
		return add(value, MILLIS_PER_DAY);
	}

	public DateTime addHours(long value) {
		return add(value, MILLIS_PER_HOUR);
	}

	public DateTime addMinutes(long value) {
		return add(value, MILLIS_PER_MINUTE);
	}

	public DateTime addSeconds(long value) {
		return add(value, MILLIS_PER_SECOND);
	}

	public DateTime addMilliseconds(long value) {
		return add(value, 1);
	}

	public DateTime addTicks(long value) {
		return new DateTime(ticks + value);
	}

	private DateTime add(long value, int scale) {
		long millis = value * scale;
		return new DateTime(ticks + millis * TICKS_PER_MILLISECOND);
	}

	public TimeSpan subtract(DateTime value) {
		return new TimeSpan(ticks - value.ticks);
	}

	public DateTime subtract(TimeSpan value) {
		return new DateTime(ticks - value.ticks);
	}

	public static boolean isLeapYear(int year) {
		return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
	}

	public static int daysInMonth(int year, int month) {
		int[] days = isLeapYear(year) ? DAYS_TO_MONTH_366 : DAYS_TO_MONTH_365;
		return days[month] - days[month - 1];
	}

	public DateTime datePart()
	{
		return new DateTime(dateToTicks(this.year(), this.month(), this.day()));
	}

	public DateTime timePart()
	{
		long ticks = timeToTicks(this.hour(), this.minute(), this.second());
		if (this.millisecond() < 0 || this.millisecond() >= MILLIS_PER_SECOND) {
			throw new StreamException("Invalid milliseconds.");
		}
		ticks += this.millisecond() * TICKS_PER_MILLISECOND;
		return new DateTime(ticks);
	}

	@Override
	public boolean equals(Object value) {
		if (value == null) {
			return false;
		}
		if (value == this) {
			return true;
		}
		if (!(value instanceof DateTime)) {
			return false;
		}
		return ((DateTime) value).ticks == ticks;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(ticks);
	}

	@Override
	public String toString() {
		return toString(null);
	}

	public String toString(String format) {
		DateTimeWriter writer = new DateTimeWriter();
		writer.write(this, format);
		return writer.toString();
	}

	public Date toSqlDate() {
		return new Date(ticksToUnixTime(ticks));
	}

	public Time toSqlTime() {
		return new Time(ticksToUnixTime(ticks));
	}

	public Timestamp toSqlTimestamp() {
		return new Timestamp(ticksToUnixTime(ticks));
	}

	@Override
	public int compareTo(DateTime dateTime) {
		if (this.ticks == dateTime.ticks) {
			return 0;
		}
		return this.ticks < dateTime.ticks ? -1 : 1;
	}

	public static DateTime utcNow()
	{
		return new DateTime();
	}

	public static DateTime now()
	{
		TimeZone timeZone = Calendar.getInstance().getTimeZone();
		int timeZoneOffset = timeZone.getRawOffset() / (3600 * 1000);
		return DateTime.utcNow().addHours(timeZoneOffset);
	}

	// TODO: to be updated.
	public static DateTime parse(String text) {
		return parse(text, false);
	}

	public static DateTime parse(String text, boolean isUSDate) {
		return new DateTimeParser(text, isUSDate).read();
	}

	public static DateTime today() {
		long ticks = currentTicks();
		int year = datePart(ticks, DATE_PART_YEAR);
		int month = datePart(ticks, DATE_PART_MONTH);
		int day = datePart(ticks, DATE_PART_DAY);
		return new DateTime(dateToTicks(year, month, day));
	}

	private static int datePart(long ticks, int part) {
		int n = (int) (ticks / TICKS_PER_DAY);
		int y400 = n / DAYS_PER_400_YEARS;
		n -= y400 * DAYS_PER_400_YEARS;
		int y100 = n / DAYS_PER_100_YEARS;
		if (y100 == 4) {
			y100 = 3;
		}
		n -= y100 * DAYS_PER_100_YEARS;
		int y4 = n / DAYS_PER_4_YEARS;
		n -= y4 * DAYS_PER_4_YEARS;
		int y1 = n / DAYS_PER_YEAR;
		if (y1 == 4) {
			y1 = 3;
		}
		if (part == DATE_PART_YEAR) {
			return y400 * 400 + y100 * 100 + y4 * 4 + y1 + 1;
		}
		n -= y1 * DAYS_PER_YEAR;
		if (part == DATE_PART_DAY_OF_YEAR) {
			return n + 1;
		}
		boolean leapYear = y1 == 3 && (y4 != 24 || y100 == 3);
		int[] days = leapYear ? DAYS_TO_MONTH_366 : DAYS_TO_MONTH_365;
		int m = n >> 5 + 1;
		while (n >= days[m]) {
			m++;
		}
		if (part == DATE_PART_MONTH) {
			return m;
		}
		return n - days[m - 1] + 1;
	}

	private static long dateToTicks(int year, int month, int day) {
		if (year >= 1 && year <= 9999 && month >= 1 && month <= 12) {
			int[] days = isLeapYear(year) ? DAYS_TO_MONTH_366
					: DAYS_TO_MONTH_365;
			if (day >= 1 && day <= days[month] - days[month - 1]) {
				int y = year - 1;
				int n = y * 365 + y / 4 - y / 100 + y / 400 + days[month - 1]
						+ day - 1;
				return n * TICKS_PER_DAY;
			}
		}
		throw new StreamException("Invalid date.");
	}

	private static long timeToTicks(int hour, int minute, int second) {
		if (hour >= 0 && hour < 24 && minute >= 0 && minute < 60 && second >= 0
				&& second < 60) {
			return TimeSpan.timeToTicks(hour, minute, second);
		}
		throw new StreamException("Invalid time.");
	}

	private static long unixTimeToTicks(long milliseconds) {
		return UNIX_EPOCH_TICKS + milliseconds * TICKS_PER_MILLISECOND;
	}

	private static long ticksToUnixTime(long ticks) {
		return (ticks - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND;
	}

	private static long currentTicks() {
		return unixTimeToTicks(System.currentTimeMillis());
	}
}