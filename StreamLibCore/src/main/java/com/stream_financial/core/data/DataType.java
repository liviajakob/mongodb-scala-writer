/* This file is proprietary software and may not in any way be copied,
 * used or distributed, without explicit permission.
 *
 * Copyright (C) Stream Financial Ltd., 2018. All rights reserved.
 */

package com.stream_financial.core.data;

import java.sql.Types;
import java.util.*;

import com.stream_financial.core.StreamException;
import com.stream_financial.core.precision.Decimal;
import com.stream_financial.core.datetime.DateTime;

public enum DataType {

	None(Void.class, "None", Types.NULL, 0), // None
	Integer(Integer.class, "Integer", Types.INTEGER, 1), // Integer
	Double(Double.class, "Double", Types.DOUBLE, 2), // Double
	String(String.class, "String", Types.VARCHAR, 3), // String
	DateTime(DateTime.class, "DateTime", Types.TIMESTAMP, 4), // DateTime
	Boolean(Boolean.class, "Boolean", Types.BOOLEAN, 5), // Boolean
	Blob(Blob.class, "BlobType", Types.BLOB, 6), // Blob
	Decimal(Decimal.class, "Decimal", Types.DECIMAL, 7), // Decimal
	Long(Long.class, "Long", Types.BIGINT, 8), // Long
	WideString(String.class, "WideString", Types.VARCHAR, 9), // WideString
	Variant(Object.class, "Variant", Types.OTHER, 10), // Variant
	Collection(List.class, "Collection", Types.OTHER, 11), // Collection
	VectorDouble512(double[].class, "VectorDouble<512>", Types.OTHER, 51), // Collection
	VectorDouble513(double[].class, "VectorDouble<513>", Types.OTHER, 52), // Collection
	VectorDouble1024(double[].class, "VectorDouble<1024>", Types.OTHER, 53), // Collection
	VectorDoubleFlex265(double[].class, "VectorDoubleFlex<265>", Types.OTHER, 54), // Collection
	VectorDoubleFlex525(double[].class, "VectorDoubleFlex<525>", Types.OTHER, 55), // Collection
	VectorDoubleFlex800(double[].class, "VectorDoubleFlex<800>", Types.OTHER, 56), // Collection
	VectorFloatFlex32767(float[].class, "VectorFloatFlex<32767>", Types.OTHER, 57); // Collection

	private static final DataType[] VALUES = {
		None,
		Integer,
		Double,
		String,
		DateTime,
		Boolean,
		Blob,
		Decimal,
		Long,
		WideString,
		Variant,
		Collection,
		VectorDouble512,
		VectorDouble513,
		VectorDouble1024,
		VectorDoubleFlex265,
		VectorDoubleFlex525,
		VectorDoubleFlex800,
		VectorFloatFlex32767,
	};

	private static final String[] DISPLAY_NAMES = {
		"None",
		"Integer",
		"Double",
		"String",
		"DateTime",
		"Boolean",
		"Blob",
		"Decimal",
		"Long",
		"Wide String",
		"Variant",
		"Collection",
		"VectorDouble<512>",
		"VectorDouble<513>",
		"VectorDouble<1024>",
		"VectorDoubleFlex<265>",
		"VectorDoubleFlex<525>",
		"VectorDoubleFlex<800>",
		"VectorFloatFlex<32767>"};

	private final Class type;
	private final String name;
	private final int sqlType;
	private final int value;

	private static final Map<Class, DataType> classToType = new HashMap<>();
	private static final Map<Integer, DataType> valueToType = new HashMap<>();
	private static final Map<String, DataType> nameToType = new TreeMap<>(java.lang.String.CASE_INSENSITIVE_ORDER);

	DataType(Class type, String name, int sqlType, int value) {
		this.type = type;
		this.name = name;
		this.sqlType = sqlType;
		this.value = value;
	}

	public int value() {
		return value;
	}

	public static DataType valueOf(int value) {
		return valueToType.get(value);
	}

	public static DataType of(Object value) {
		return value == null ? DataType.None : of(value.getClass());
	}

	public static DataType of(Class value) {
		DataType type = classToType.get(value);
		if (type == null) {
			throw new StreamException("Failed to map class '%s' to type.",
					value);
		}
		return type;
	}

	public static DataType parse(String name) {
		return nameToType.get(name);
	}

	@Override
	public String toString() {
		return name;
	}

	public int sqlType() {
		return sqlType;
	}

	public Class equivalentClass() {
		return type;
	}

	public String displayName() {
		return DISPLAY_NAMES[value];
	}

	public boolean numeric() {
		return this == Integer || this == Double || this == Long
				|| this == Decimal;
	}

	static {
		for (DataType value : VALUES) {
			if (!classToType.containsKey(value.type)) {
				classToType.put(value.type, value);
			}
			valueToType.put(value.value, value);
			nameToType.put(value.name, value);
		}
	}
}