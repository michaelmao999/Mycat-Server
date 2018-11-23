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
 * 
 * @author mycat
 */
public class PartitionByFileMap extends AbstractPartitionAlgorithm implements RuleAlgorithm {

	private String mapFile;
	private Map<Object, Integer> app2Partition;
	private List<String> sortedKeys;
	/**
	 * Map<Object, Integer> app2Partition中key值的类型：默认值为0，0表示Integer，非零表示String
	 */
	private int type;
	
	/**
	 * 默认节点在map中的key
	 */
	private static final String DEFAULT_NODE = "DEFAULT_NODE";
	
	/**
	 * 默认节点:小于0表示不设置默认节点，大于等于0表示设置默认节点
	 * 
	 * 默认节点的作用：枚举分片时，如果碰到不识别的枚举值，就让它路由到默认节点
	 *                如果不配置默认节点（defaultNode值小于0表示不配置默认节点），碰到
	 *                不识别的枚举值就会报错，
	 *                like this：can't find datanode for sharding column:column_name val:ffffffff    
	 */
	private int defaultNode = -1;

	@Override
	public void init() {

		initialize();
	}

	public void init4Test(Map<Object, Integer> app2Partition) {
		this.app2Partition = app2Partition;
		sortedKeys = new ArrayList<String>();
		Iterator<Object> iter = app2Partition.keySet().iterator();
		while (iter.hasNext()) {
			sortedKeys.add(iter.next() + "");
		}
		Collections.sort(sortedKeys);

	}

	public void setMapFile(String mapFile) {
		this.mapFile = mapFile;
	}
	
	public void setType(int type) {
		this.type = type;
	}

	public void setDefaultNode(int defaultNode) {
		this.defaultNode = defaultNode;
	}

	@Override
	public Integer calculate(String columnValue)  {
		try {
			Object value = columnValue;
			if (type == 0) {
				value = Integer.valueOf(columnValue);
			}
			Integer rst = null;
			Integer pid = app2Partition.get(value);
			if (pid != null) {
				rst = pid;
			} else {
				rst = app2Partition.get(DEFAULT_NODE);
			}
			return rst;
		} catch (NumberFormatException e){
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please check if the format satisfied.").toString(),e);
		}
	}

	/**
	 * 返回所有被路由到的节点的编号
	 * 返回长度为0的数组表示所有节点都被路由（默认）
	 * 返回null表示没有节点被路由到
	 */
	@Override
	public Integer[] calculateRange(String beginValue, String endValue, int rangeType)  {
		List<Integer> hashList = scanInRange(beginValue, endValue, rangeType);
		if (hashList.isEmpty()) {
			return new Integer[0];
		}
		Integer[] result = new Integer[hashList.size()];
		return hashList.toArray(result);
	}

	private List<Integer> scanInRange(String beginValue, String endValue, int rangeType) {
		List<Integer> hashList = new ArrayList<>();
		int len = sortedKeys.size();
		boolean isMatch = false;
		for (int index = 0; index < len; index++) {
			String key = sortedKeys.get(index);
			if (isMatch) {
				hashList.add(app2Partition.get(key));
			} else {
				int compareResult = isInRange(beginValue, endValue, rangeType, key);
				//在范围内
				if (compareResult == 0 || compareResult == 100) {
					hashList.add(app2Partition.get(Integer.valueOf(key)));
				}
				if (compareResult == 100) {
					isMatch = true;
				} else if (compareResult > 0) {
					break;
				}
			}
		}
		return hashList;
	}

	private static int isInRange(String beginValue, String endValue, int rangeType, String value) {
		int from = 1;
		if (beginValue != null && beginValue.length() > 0) {

			//特殊处理不等于操作符 (!=)
			if (RangeValue.NOT == rangeType) {
				String[] dataList = beginValue.split(",");
				boolean isMatched = isContain(dataList, value);
			    if (isMatched) {
			        //两个值相等，就忽略
			        return -1;
                } else {
			        //否则全命中
			        return 0;
                }
            } else {
				from = value.compareTo(beginValue);
			}
			if (from > 0 && endValue == null) {
				//always match
				return 100;
			}
		}
		int to = -1;
		if (endValue != null && endValue.length() > 0) {
			to = value.compareTo(endValue);
		}
		if (to > 0) {
			return to;
		}
		if (RangeValue.EE == rangeType && (from >= 0 && to <= 0)) {
			return 0;
		} else if (RangeValue.EN == rangeType && (from >= 0 && to < 0)) {
			return 0;
		} else if (RangeValue.NE == rangeType && (from > 0 && to <= 0)) {
			return 0;
		}else if (RangeValue.NN == rangeType && (from > 0 && to < 0)) {
			return 0;
		}
		return -1;
	}

	private static boolean isContain(String[] dataList, String value) {
		if (dataList == null || dataList.length == 0) {
			return false;
		}
		if (dataList.length == 1) {
			return dataList[0].equals(value);
		}
		//[1000  1001]
		for (String data : dataList) {
			if (data.trim().equals(value)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int getPartitionNum() {
		Set<Integer> set = new HashSet<Integer>(app2Partition.values());
		int count = set.size();
		return count;
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
			
			app2Partition = new HashMap<Object, Integer>();
			
			for (String line = null; (line = in.readLine()) != null;) {
				line = line.trim();
				if (line.startsWith("#") || line.startsWith("//")) {
					continue;
				}
				int ind = line.indexOf('=');
				if (ind < 0) {
					continue;
				}
				try {
					String key = line.substring(0, ind).trim();
					int pid = Integer.parseInt(line.substring(ind + 1).trim());
					if(type == 0) {
						app2Partition.put(Integer.parseInt(key), pid);
					} else {
						app2Partition.put(key, pid);
					}
				} catch (Exception e) {
				}
			}
			//设置默认节点
			if(defaultNode >= 0) {
				app2Partition.put(DEFAULT_NODE, defaultNode);
			}
			//基于key排序，稍后可用于range查找.
			sortedKeys = new ArrayList<String>();
			Iterator<Object> iter = app2Partition.keySet().iterator();
			while (iter.hasNext()) {
				sortedKeys.add(iter.next() + "");
			}
			Collections.sort(sortedKeys);
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

}