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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.parquet.io.api.Binary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A subclass of {@link ParquetInputFormat} to read from Parquets files and convert to {@link Map} type.
 * It is mainly used to read complex data type with nested fields.
 */
public class ParquetMapInputFormat extends ParquetInputFormat<Map> {

	public ParquetMapInputFormat(Path path, TypeInformation[] fieldTypes, String[] fieldNames) {
		super(path, fieldTypes, fieldNames);
	}

	@Override
	protected Map convert(Row row) {
		Map<String, Object> map = new HashMap<>();
		convert(map, row, fieldTypes, fieldNames);
		return map;
	}

	@SuppressWarnings("unchecked")
	private void convert(Map<String, Object> map, Row row, TypeInformation<?>[] fieldTypes, String[] fieldNames) {
		for (int i = 0; i < fieldNames.length; i++) {
			if (row.getField(i) != null) {
				if (fieldTypes[i].equals(BasicTypeInfo.STRING_TYPE_INFO)) {
					map.put(fieldNames[i], ((Binary) row.getField(i)).toStringUsingUTF8());
				} else if (fieldTypes[i].equals(BasicTypeInfo.DATE_TYPE_INFO)) {
					map.put(fieldNames[i], new java.util.Date((long) row.getField(i)));
				} else if (fieldTypes[i].equals(BasicTypeInfo.INSTANT_TYPE_INFO)) {
					if (row.getField(i) instanceof Binary) {
						map.put(fieldNames[i], bigIntToTimestamp(((Binary) row.getField(i))));
					} else {
						map.put(fieldNames[i], microsecsToTimestamp((long) row.getField(i)));
					}
				} else if (fieldTypes[i] instanceof BasicTypeInfo
					|| fieldTypes[i] instanceof PrimitiveArrayTypeInfo
					|| fieldTypes[i] instanceof BasicArrayTypeInfo) {
						map.put(fieldNames[i], row.getField(i));
				} else if (fieldTypes[i] instanceof SqlTimeTypeInfo) {
					map.put(fieldNames[i], new java.sql.Date((int) row.getField(i) * MILLIS_IN_DAY));
				} else if (fieldTypes[i] instanceof RowTypeInfo) {
					Map<String, Object> nestedRow = new HashMap<>();
					RowTypeInfo nestedRowTypeInfo = (RowTypeInfo) fieldTypes[i];
					convert(nestedRow, (Row) row.getField(i),
						nestedRowTypeInfo.getFieldTypes(), nestedRowTypeInfo.getFieldNames());
					map.put(fieldNames[i], nestedRow);
				} else if (fieldTypes[i] instanceof MapTypeInfo) {
					Map<String, Object> nestedMap = new HashMap<>();
					MapTypeInfo mapTypeInfo = (MapTypeInfo) fieldTypes[i];
					convert(nestedMap, (Map<String, Object>) row.getField(i), mapTypeInfo);
					map.put(fieldNames[i], nestedMap);
				} else if (fieldTypes[i] instanceof ObjectArrayTypeInfo) {
					List<Object> nestedObjectList = new ArrayList<>();
					ObjectArrayTypeInfo objectArrayTypeInfo = (ObjectArrayTypeInfo) fieldTypes[i];
					convert(nestedObjectList, (Row[]) row.getField(i), objectArrayTypeInfo);
					map.put(fieldNames[i], nestedObjectList);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void convert(Map<String, Object> target, Map<String, Object> source, MapTypeInfo mapTypeInfo) {
		TypeInformation valueTypeInfp = mapTypeInfo.getValueTypeInfo();

		for (String key : source.keySet()) {
			if (valueTypeInfp instanceof RowTypeInfo) {
				Map<String, Object> nestedRow = new HashMap<>();
				convert(nestedRow, (Row) source.get(key),
					((RowTypeInfo) valueTypeInfp).getFieldTypes(), ((RowTypeInfo) valueTypeInfp).getFieldNames());
				target.put(key, nestedRow);
			} else if (valueTypeInfp instanceof MapTypeInfo) {
				Map<String, Object> nestedMap = new HashMap<>();
				convert(nestedMap, (Map<String, Object>) source.get(key), (MapTypeInfo) valueTypeInfp);
				target.put(key, nestedMap);
			} else if (valueTypeInfp instanceof ObjectArrayTypeInfo) {
				List<Object> nestedObjectList = new ArrayList<>();
				convert(nestedObjectList, (Object[]) source.get(key), (ObjectArrayTypeInfo) valueTypeInfp);
				target.put(key, nestedObjectList);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void convert(List<Object> target, Object[] source, ObjectArrayTypeInfo objectArrayTypeInfo) {
		TypeInformation<?> itemType = objectArrayTypeInfo.getComponentInfo();
		for (int i = 0; i < source.length; i++) {
			if (itemType instanceof RowTypeInfo) {
				Map<String, Object> nestedRow = new HashMap<>();
				convert(nestedRow, (Row) source[i],
					((RowTypeInfo) itemType).getFieldTypes(), ((RowTypeInfo) itemType).getFieldNames());
				target.add(nestedRow);
			} else if (itemType instanceof MapTypeInfo) {
				Map<String, Object> nestedMap = new HashMap<>();
				MapTypeInfo mapTypeInfo = (MapTypeInfo) itemType;
				convert(nestedMap, (Map<String, Object>) source[i], mapTypeInfo);
				target.add(nestedMap);
			} else if (itemType instanceof ObjectArrayTypeInfo) {
				List<Object> nestedObjectList = new ArrayList<>();
				convert(nestedObjectList, (Row[]) source[i], (ObjectArrayTypeInfo) itemType);
				target.add(nestedObjectList);
			}
		}

	}
}
