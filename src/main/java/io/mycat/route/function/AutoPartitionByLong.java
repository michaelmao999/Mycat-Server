/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.route.function;

import io.mycat.config.model.rule.RuleAlgorithm;
import io.mycat.sqlengine.mpp.RangeValue;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * auto partition by Long ,can be used in auto increment primary key partition
 * 
 * @author wuzhi
 */
public class AutoPartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm{

	private String mapFile;
	private LongRange[] longRongs;
	
	private int defaultNode = -1;
	@Override
	public void init() {

		initialize();
	}

	public void init4Test(List<LongRange> data) {
		longRongs = new LongRange[data.size()];
		Collections.sort(data);
		data.toArray(longRongs);
	}

	public void setMapFile(String mapFile) {
		this.mapFile = mapFile;
	}

	@Override
	public Integer calculate(String columnValue)  {
		try {
			Long newValue = Long.parseLong(columnValue);
			LongRange target = binarySearch(longRongs, newValue);
			if (target != null) { // it is maatched !
				return target.nodeIndx;
			}
			//如果这个值比最小还小，那么返回最小的节点，避免所有的节点都执行SQL
			if (newValue < longRongs[0].valueStart) {
				return null;
			}
			//如果这个值比最大还大，那么返回最大的节点，避免所有的节点都执行SQL
			if (newValue > longRongs[longRongs.length - 1].valueEnd) {
				return null;
			}
			//数据超过范围，暂时使用配置的默认节点
			if (defaultNode >= 0) {
				return defaultNode;
			}
			return null;
		} catch (NumberFormatException e){
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please eliminate any quote and non number within it.").toString(),e);
		}
	}
	
	@Override
	public Integer[] calculateRange(String beginValue, String endValue, int rangeType){
		Long beginLongValue = getNumber(beginValue, false);
		Long endLongValue = getNumber(endValue, true);
		Set<Integer> result = new HashSet<>();
		if (beginLongValue != null && endLongValue != null) {
			for ( LongRange range : longRongs) {
				if ((range.valueStart <= beginLongValue && range.valueEnd >= beginLongValue) ||
                        (range.valueStart <= endLongValue && range.valueEnd >= endLongValue) ||
                        (range.valueStart > beginLongValue && range.valueEnd < endLongValue)) {
					//between
					if (rangeType != RangeValue.NOT ) {
						result.add(range.nodeIndx);
					} else {
						//分区信息是  1-5000
						//查询条件部分落在分区内,比如: [100--3000] , not betweeen 条件也应该执行 
						if (range.valueStart < beginLongValue || range.valueEnd > endLongValue) {
							result.add(range.nodeIndx);
						}
					}
				} else if (rangeType == RangeValue.NOT) {
					// not between
					result.add(range.nodeIndx);
				}
			}
		} else if (beginLongValue != null) {  // >= smllerOne
			for ( LongRange range : longRongs) {
				if (range.valueEnd >= beginLongValue) {
					result.add(range.nodeIndx);
				}
			}
		} else if (endLongValue != null) {		// <= biggerOne
			for ( LongRange range : longRongs) {
				if (range.valueStart <= endLongValue) {
					result.add(range.nodeIndx);
				}
			}
		}
		if (result.isEmpty()) {
            //没有找到任何合适的节点
            Integer[] result2 = new Integer[1];
            result2[0] = Integer.MAX_VALUE;
            return result2;
        }
		return result.toArray(new Integer[result.size()]);
	}

	private static Integer[] getAllNodes(LongRange[] longRongs) {
		Set<Integer> filterDuplicatedNode = new HashSet<>();
		for (LongRange range : longRongs) {
			filterDuplicatedNode.add(range.nodeIndx);
		}
		return filterDuplicatedNode.toArray(new Integer[filterDuplicatedNode.size()]);
	}
	
	private static LongRange getRangeByValue(LongRange[] longRongs, Long longValue) {
		if (longValue == null) {
			return null;
		}
		return binarySearch(longRongs, longValue);
	}
	
	private static Long getNumber(String value, boolean isGetBiggest) {
		if (value == null || value.isEmpty()) {
			return null;
		}
		Long result = null;
		String[] dataList = value.split(",");
		try {
			for (String data : dataList) {
				Long newValue = Long.valueOf(data);
				if (result == null) {
					result = newValue;
				} else {
					if (isGetBiggest) {
						//寻找一个最大值
						if (newValue.longValue() > result.longValue()) {
							result = newValue;
						}
					} else {
						//寻找一个最小值
						if (newValue.longValue() < result.longValue()) {
							result = newValue;
						}
					}
				}
			}
		} catch (NumberFormatException e){
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(value).append(" Please eliminate any quote and non number within it.").toString(),e);
		}
		return result;
	}
	
	@Override
	public int getPartitionNum() {
//		int nPartition = longRongs.length;
		
		/*
		 * fix #1284 这里的统计应该统计Range的nodeIndex的distinct总数
		 */
		Set<Integer> distNodeIdxSet = new HashSet<Integer>();
		for(LongRange range : longRongs) {
			distNodeIdxSet.add(range.nodeIndx);
		}
		int nPartition = distNodeIdxSet.size();
		return nPartition;
	}

	private void initialize() {
		BufferedReader in = null;
		try {
			// FileInputStream fin = new FileInputStream(new File(fileMapPath));
			InputStream fin = this.getClass().getClassLoader()
					.getResourceAsStream(mapFile);
			if (fin == null) {
				throw new RuntimeException("can't find class resource file "
						+ mapFile);
			}
			in = new BufferedReader(new InputStreamReader(fin));
			LinkedList<LongRange> longRangeList = new LinkedList<LongRange>();

			for (String line = null; (line = in.readLine()) != null;) {
				line = line.trim();
				if (line.startsWith("#") || line.startsWith("//")) {
					continue;
				}
				int ind = line.indexOf('=');
				if (ind < 0) {
					System.out.println(" warn: bad line int " + mapFile + " :"
							+ line);
					continue;
				}
					String pairs[] = line.substring(0, ind).trim().split("-");
					long longStart = NumberParseUtil.parseLong(pairs[0].trim());
					long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
					int nodeId = Integer.parseInt(line.substring(ind + 1)
							.trim());
					longRangeList
							.add(new LongRange(nodeId, longStart, longEnd));

			}
			Collections.sort(longRangeList);
			longRongs = longRangeList.toArray(new LongRange[longRangeList
					.size()]);
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}

		} finally {
			try {
				in.close();
			} catch (Exception e2) {
			}
		}
	}
	
	public int getDefaultNode() {
		return defaultNode;
	}

	public void setDefaultNode(int defaultNode) {
		this.defaultNode = defaultNode;
	}

	static class LongRange implements Comparable<LongRange>{
		public final int nodeIndx;
		public final long valueStart;
		public final long valueEnd;

		public LongRange(int nodeIndx, long valueStart, long valueEnd) {
			super();
			this.nodeIndx = nodeIndx;
			this.valueStart = valueStart;
			this.valueEnd = valueEnd;
		}

		public int isMatch(long value) {
			if (value < valueStart) {
				return 1;
			}
			if (value > valueEnd) {
				return -1;
			}
			return 0;
		}
		/**
		 *  重载hashCode
		 * @return
		 */
		@Override
		public int hashCode() {
			if (valueStart == valueEnd) {
				return (int) valueStart;
			} else {
				return (int) ((valueStart + valueEnd) /2);
			}
		}

		/**
		 * 重载equals方法，只要startValue和endValue相等，对象就相同
		 * @param obj
		 * @return
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj instanceof LongRange) {
				LongRange range = (LongRange) obj;
				return (valueStart == range.valueStart && valueEnd == range.valueEnd);
			} else {
				return false;
			}
		}

		@Override
		public int compareTo(LongRange range) {
			if (valueStart > range.valueStart) {
				return 1;
			} else if (valueStart == range.valueStart) {
				return 0;
			}
			return -1;
		}

		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append(this.nodeIndx).append(" [" ).append(this.valueStart).append('-').append(this.valueEnd).append(']');
			return builder.toString();
		}
	}

	public static LongRange binarySearch(LongRange[] data, long value) {
		int low = 0;
		int high = data.length - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			int cmp = data[mid].isMatch(value);

			if (cmp < 0)
				low = mid + 1;
			else if (cmp > 0)
				high = mid - 1;
			else
				return data[mid]; // key found
		}
		return null;  // key not found.

	}


	public static void main(String[] argc) throws  Exception {
		List<LongRange> list = new ArrayList<>();
		list.add(new LongRange(1, 101, 200));
		list.add(new LongRange(0, 1, 100));
		list.add(new LongRange(2, 201, 400));

		Collections.sort(list);
		LongRange[] longRongs = list.toArray(new LongRange[list.size()]);

		LongRange result = binarySearch(longRongs, 1);
		System.out.println( "1 in range " + result);

		result = binarySearch(longRongs, 0);
		System.out.println( "0 in range " + result);

		result = binarySearch(longRongs, 200);
		System.out.println( "200 in range " + result);

		result = binarySearch(longRongs, 201);
		System.out.println( "201 in range " + result);

		result = binarySearch(longRongs, 400);
		System.out.println( "400 in range " + result);

		result = binarySearch(longRongs, 401);
		System.out.println( "401 in range " + result);

		System.out.println(list);

	}
}