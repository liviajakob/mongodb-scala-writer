/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.precision;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;

import com.stream_financial.core.StreamException;

public class Decimal {

	private final int low;
	private final int mid;
	private final int high;
	private final boolean isNegative;
	private final byte scale;

	public static final Decimal Zero = new Decimal(0, 0, 0,  false, (byte) 0);
	public static final Decimal One = new Decimal(1,  0, 0, false, (byte) 0);
	public static final Decimal MinusOne = new Decimal(1,  0, 0, true, (byte) 0);
	public static final Decimal MinValue = new Decimal(-1, -1, -1, true, (byte) 0);
	public static final Decimal MaxValue = new Decimal(-1, -1, -1, false, (byte) 0);

	// Used to ensure that BigDecimal numbers can be safely converted to Decimal
	public static MathContext SafeRounding = new MathContext(29, RoundingMode.HALF_UP);

	public Decimal(int low, int mid, int high, boolean isNegative, byte scale) {
		this.low = low;
		this.mid = mid;
		this.high = high;
		this.isNegative = isNegative;
		this.scale = scale;
	}

	public Decimal(byte value) {
		if (value > 0) {
			this.low = value;
			this.isNegative = false;
		} else if (value == 0) {
			this.low = 0;
			this.isNegative = false;
		} else {
			this.low = -value;
			this.isNegative = true;
		}
		this.mid = 0;
		this.high = 0;
		this.scale = 0;
	}

	public Decimal(BigDecimal value) {
		// Negative scale not supported, scaling up to 0.
		if (value.scale() < 0) {
			value = value.setScale(0, RoundingMode.HALF_UP);
		}

        value = value.round(SafeRounding);

		// Unscaled (absolute) value.
//		BigInteger unscaled = value.abs().unscaledValue();
//		if (unscaled.bitLength() > 96) {
//			throw new StreamException(
//					"The BigDecimal value '%s' is too large to be converted to a Decimal.",
//					value.toPlainString());
//		}
		BigInteger unscaled = value.abs().unscaledValue();
		byte[] b = unscaled.toByteArray();
		int i = b.length;

		// Low.
		int low = 0;
		if (--i >= 0)
			low |= (b[i] & 0xFF);
		if (--i >= 0)
			low |= ((b[i] & 0xFF) << 8);
		if (--i >= 0)
			low |= ((b[i] & 0xFF) << 16);
		if (--i >= 0)
			low |= ((b[i] & 0xFF) << 24);
		this.low = low;

		int mid = 0;
		if (--i >= 0)
			mid |= (b[i] & 0xFF);
		if (--i >= 0)
			mid |= ((b[i] & 0xFF) << 8);
		if (--i >= 0)
			mid |= ((b[i] & 0xFF) << 16);
		if (--i >= 0)
			mid |= ((b[i] & 0xFF) << 24);
		this.mid = mid;

		// High.
		int high = 0;
		if (--i >= 0)
			high |= (b[i] & 0xFF);
		if (--i >= 0)
			high |= ((b[i] & 0xFF) << 8);
		if (--i >= 0)
			high |= ((b[i] & 0xFF) << 16);
		if (--i >= 0)
			high |= ((b[i] & 0xFF) << 24);
		this.high = high;

		// Scale/sign.
		int scale = value.scale();
		if (scale > 28) {
			throw new StreamException(
					"The BigDecimal scale for '%s' is too large to be converted to a Decimal.",
					value.toPlainString());
		}

		// check scale boundary
		this.scale = (byte) scale;
		// check sign boundary
		this.isNegative = value.signum() < 0;
	}

	public Decimal(String text) {
		this(new BigDecimal(text));
	}

	public int low() {
		return low;
	}

	public int mid() {
		return mid;
	}

	public int high() {
		return high;
	}

	public boolean isNegative() {
		return isNegative;
	}

	public byte sign() {
		return isNegative ? (byte) 0x80 : (byte) 0;
	}

	public byte scale() {
		return scale;
	}

	private int sigNum() {
		if (isNegative) {
			return -1;
		}

		if (low == 0 && mid == 0 && high ==0) {
			return 0;
		}

		return 1;
	}

	public BigDecimal toBigDecimal() {
		byte[] b = new byte[12];
		b[0] = (byte) (high >>> 24);
		b[1] = (byte) (high >>> 16);
		b[2] = (byte) (high >>> 8);
		b[3] = (byte) high;
		b[4] = (byte) (mid >>> 24);
		b[5] = (byte) (mid >>> 16);
		b[6] = (byte) (mid >>> 8);
		b[7] = (byte) mid;
		b[8] = (byte) (low >>> 24);
		b[9] = (byte) (low >>> 16);
		b[10] = (byte) (low >>> 8);
		b[11] = (byte) low;
		return new BigDecimal(new BigInteger(sigNum(), b), scale);
	}

	@Override
	public String toString() {
		return toBigDecimal().toPlainString();
	}

	/*
	public byte toByte() {
		return toBigDecimal().byteValue();
	}
	*/

	@Override
	public boolean equals(Object object) {
		if (!(object instanceof Decimal)) {
			return false;
		}
		Decimal d = (Decimal) object;
		return d.low == low && d.mid == mid && d.high == high
				&& d.scale == scale && d.isNegative == isNegative;
	}

	@Override
	public int hashCode() {
		return Decimal.hashCode(this);
	}

	public static int hashCode(Decimal value) {
        return hashCombine(value.high, hashCombine(value.mid, value.low));
	}

	private static int hashCombine(int left, int right) {
        return 1013 * (Integer.hashCode(left)) ^ 1009 * (Integer.hashCode(right));
    }
}