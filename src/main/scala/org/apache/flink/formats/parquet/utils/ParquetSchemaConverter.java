/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.utils;

import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * Schema converter converts Parquet schema to and from Flink internal types.
 */
public class ParquetSchemaConverter {
	public static final String MAP_KEY = "key";
	public static final String MAP_VALUE = "value";
	public static final String LIST_ELEMENT = "array";
	public static final String MESSAGE_ROOT = "root";
	private static final AvroSchemaConverter SCHEMA_CONVERTER = new AvroSchemaConverter();

	public static TypeInformation<?> fromParquetType(MessageType type) {
		return convertFields(type.getFields());
	}

	public static MessageType toParquetType(TypeInformation<?> typeInformation, MessageType ref) {
		return (MessageType) convertField(null, typeInformation, Type.Repetition.OPTIONAL, ref);
	}

	private static TypeInformation<?> convertFields(List<Type> parquetFields) {
		List<TypeInformation<?>> types = new ArrayList<>();
		List<String> names = new ArrayList<>();
		for (Type field : parquetFields) {
			TypeInformation<?> subType = convertField(field);
			if (subType != null) {
				types.add(subType);
				names.add(field.getName());
			}
		}

		return new RowTypeInfo(types.toArray(new TypeInformation<?>[types.size()]),
			names.toArray(new String[names.size()]));
	}

	private static TypeInformation<?> convertField(final Type fieldType) {
		TypeInformation<?> typeInfo = null;
		if (fieldType.isPrimitive()) {
			PrimitiveType primitiveType = fieldType.asPrimitiveType();
			switch (primitiveType.getPrimitiveTypeName()) {
				case BINARY:
				case FIXED_LEN_BYTE_ARRAY:
					if (primitiveType.getOriginalType() != null) {
						switch (primitiveType.getOriginalType()) {
							case UTF8:
								typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
								break;
							case DECIMAL:
								typeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO;
								break;
							default:
								typeInfo = PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
						}
					} else {
						typeInfo = PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
					}
					break;
				case BOOLEAN:
					typeInfo = BasicTypeInfo.BOOLEAN_TYPE_INFO;
					break;
				case INT32:
					if (primitiveType.getOriginalType() != null
						&& primitiveType.getOriginalType() == OriginalType.DATE) {
						typeInfo = SqlTimeTypeInfo.DATE;
					} else {
						typeInfo = BasicTypeInfo.INT_TYPE_INFO;
					}
					break;
				case INT64:
					if (primitiveType.getOriginalType() != null) {
						switch (primitiveType.getOriginalType()) {
							case TIMESTAMP_MILLIS:
								typeInfo = BasicTypeInfo.DATE_TYPE_INFO;
								break;
							case TIMESTAMP_MICROS:
								typeInfo = BasicTypeInfo.INSTANT_TYPE_INFO;
								break;
							default:
								typeInfo = BasicTypeInfo.LONG_TYPE_INFO;
						}
					} else {
						typeInfo = BasicTypeInfo.LONG_TYPE_INFO;
					}
					break;
				case INT96:
					// https://issues.apache.org/jira/browse/PARQUET-323
					// INT96 is only used for storing timestamp, and is deprecated.
					// Here it will be converted to MICROSECS in a java.sql.Timestamp
					typeInfo = BasicTypeInfo.INSTANT_TYPE_INFO;
					break;
				case FLOAT:
					typeInfo = BasicTypeInfo.FLOAT_TYPE_INFO;
					break;
				case DOUBLE:
					typeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO;
					break;
				default:
					throw new UnsupportedOperationException("Unsupported schema: " + fieldType);
			}
		} else {
			GroupType parquetGroupType = fieldType.asGroupType();
			OriginalType originalType = parquetGroupType.getOriginalType();
			if (originalType != null) {
				switch (originalType) {
					case LIST:
						if (parquetGroupType.getFieldCount() != 1) {
							throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
						}
						Type repeatedType = parquetGroupType.getType(0);
						if (!repeatedType.isRepetition(Type.Repetition.REPEATED)) {
							throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
						}

						if (repeatedType.isPrimitive()) {
							typeInfo = BasicArrayTypeInfo.getInfoFor(
								Array.newInstance(convertField(repeatedType).getTypeClass(), 0).getClass());
						} else {
							typeInfo = ObjectArrayTypeInfo.getInfoFor(convertField(repeatedType));
						}
						break;

					case MAP_KEY_VALUE:
					case MAP:
						if (parquetGroupType.getFieldCount() != 1 || parquetGroupType.getType(0).isPrimitive()) {
							throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
						}

						GroupType mapKeyValType = parquetGroupType.getType(0).asGroupType();
						if (!mapKeyValType.isRepetition(Type.Repetition.REPEATED)
							|| mapKeyValType.getFieldCount() != 2) {
							throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
						}
						Type keyType = mapKeyValType.getType(0);
						if (!keyType.isPrimitive()
							|| !keyType.asPrimitiveType().getPrimitiveTypeName().equals(
								PrimitiveType.PrimitiveTypeName.BINARY)
							|| !keyType.getOriginalType().equals(OriginalType.UTF8)) {
							throw new IllegalArgumentException("Map key type must be binary (UTF8): "
								+ keyType);
						}

						Type valueType = mapKeyValType.getType(1);
						return new MapTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, convertField(valueType));
					default:
						throw new UnsupportedOperationException("Unsupported schema: " + fieldType);
				}
			} else {
				// if no original type than it is a record
				return convertFields(parquetGroupType.getFields());
			}
		}

		return typeInfo;
	}

	private static Type convertField(String fieldName, TypeInformation<?> typeInfo, Type.Repetition inheritRepetition, Type ref) {
		Type fieldType = null;

		Type.Repetition repetition = inheritRepetition == null ? Type.Repetition.OPTIONAL : inheritRepetition;
//		if (typeInfo.isBasicType()) {
//			BasicTypeInfo basicTypeInfo = (BasicTypeInfo) typeInfo;
//			if (basicTypeInfo.equals(BasicTypeInfo.BIG_DEC_TYPE_INFO)
//				|| basicTypeInfo.equals(BasicTypeInfo.BIG_INT_TYPE_INFO)) {
//				fieldType = Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).named(fieldName);
//			} else if (basicTypeInfo.equals(BasicTypeInfo.INT_TYPE_INFO)) {
//				fieldType = Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).named(fieldName);
//			} else if (basicTypeInfo.equals(BasicTypeInfo.DOUBLE_TYPE_INFO)) {
//				fieldType = Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).named(fieldName);
//			} else if (basicTypeInfo.equals(BasicTypeInfo.FLOAT_TYPE_INFO)) {
//				fieldType = Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition).named(fieldName);
//			} else if (basicTypeInfo.equals(BasicTypeInfo.LONG_TYPE_INFO)) {
//				fieldType = Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).named(fieldName);
//			} else if (basicTypeInfo.equals(BasicTypeInfo.SHORT_TYPE_INFO)) {
//				fieldType = new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT32, fieldName, OriginalType.INT_16);
//			} else if (basicTypeInfo.equals(BasicTypeInfo.BYTE_TYPE_INFO)) {
//				fieldType = new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT32, fieldName, OriginalType.INT_8);
//			} else if (basicTypeInfo.equals(BasicTypeInfo.CHAR_TYPE_INFO)) {
//				fieldType = new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT32, fieldName, OriginalType.UINT_16);
//			} else if (basicTypeInfo.equals(BasicTypeInfo.BOOLEAN_TYPE_INFO)) {
//				fieldType = Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).named(fieldName);
//			} else if (basicTypeInfo.equals(BasicTypeInfo.DATE_TYPE_INFO)) {
//				fieldType = new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT64, fieldName, OriginalType.TIMESTAMP_MILLIS);
//			} else if (basicTypeInfo.equals(BasicTypeInfo.STRING_TYPE_INFO)) {
//				fieldType = new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BINARY, fieldName, OriginalType.UTF8);
//			}
//		} else if (typeInfo instanceof org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo) {
//			if (typeInfo.equals(SqlTimeTypeInfo.TIMESTAMP)) {
//				fieldType = new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT64, fieldName, OriginalType.TIMESTAMP_MICROS);
//			} else {
//				fieldType = new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT64, fieldName, OriginalType.TIMESTAMP_MILLIS);
//			}
		if (typeInfo.isBasicType()
			|| typeInfo instanceof SqlTimeTypeInfo
			|| typeInfo instanceof MapTypeInfo
			|| typeInfo instanceof ObjectArrayTypeInfo
			|| typeInfo instanceof PrimitiveArrayTypeInfo
			|| typeInfo instanceof BasicArrayTypeInfo) {
			fieldType = ref;
		} else {
			RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;
			List<Type> types = new ArrayList<>();
			String[] fieldNames = rowTypeInfo.getFieldNames();
			TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();

			GroupType currentRef = (GroupType) ref;
			for (int i = 0; i < rowTypeInfo.getArity(); i++) {
				types.add(convertField(fieldNames[i], fieldTypes[i], Type.Repetition.OPTIONAL,
					currentRef.getFields().get(currentRef.getFieldIndex(fieldNames[i]))));
			}
			if (fieldName == null) {
				fieldType = new MessageType(MESSAGE_ROOT, types);
			} else {
				fieldType = new GroupType(repetition, fieldName, types);
			}
		}

		return fieldType;
	}

	private boolean isElementType(Type repeatedType, String parentName) {
		return (
			// can't be a synthetic layer because it would be invalid
			repeatedType.isPrimitive()
				|| repeatedType.asGroupType().getFieldCount() > 1
				|| repeatedType.asGroupType().getType(0).isRepetition(Type.Repetition.REPEATED)
				// known patterns without the synthetic layer
				|| repeatedType.getName().equals("array")
				|| repeatedType.getName().equals(parentName + "_tuple"));
	}
}
