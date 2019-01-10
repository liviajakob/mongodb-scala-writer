/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core;

public class StreamException extends RuntimeException
{
	public StreamException(String message) {
		super(message);
	}

	public StreamException(String format, Object... parameters) {
		this(String.format(format, parameters));
	}

	public StreamException(Exception exception) {
		super(format(exception), exception);
	}

	public StreamException(String message, Exception exception) {
		super(message, exception);
	}

	public static String format(Exception exception) {
		if (exception == null) {
			return null;
		}
		String message = exception.getMessage();
		if (message == null || message.length() == 0) {
			return exception.getClass().getCanonicalName();
		}
		return message;
	}
}