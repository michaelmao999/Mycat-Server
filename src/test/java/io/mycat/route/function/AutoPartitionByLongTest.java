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

import junit.framework.Assert;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AutoPartitionByLongTest {

	@Test
	public void test()  {
		AutoPartitionByLong autoPartition=new AutoPartitionByLong();
		autoPartition.setMapFile("autopartition-long.txt");
		autoPartition.init();
		String idVal="0";
		Assert.assertEquals(true, 0==autoPartition.calculate(idVal));
		
		idVal="2000000";
		Assert.assertEquals(true, 0==autoPartition.calculate(idVal)); 
		
		idVal="2000001";
		Assert.assertEquals(true, 1==autoPartition.calculate(idVal)); 
		
		idVal="4000000";
		Assert.assertEquals(true, 1==autoPartition.calculate(idVal)); 
		
		idVal="4000001";
		Assert.assertEquals(true, 2==autoPartition.calculate(idVal)); 
	}

	private AutoPartitionByLong getInitPartition() {
		List<AutoPartitionByLong.LongRange> list = new ArrayList<>();
		list.add(new AutoPartitionByLong.LongRange(1, 101, 200));
		list.add(new AutoPartitionByLong.LongRange(0, 1, 100));
		list.add(new AutoPartitionByLong.LongRange(2, 201, 400));

		AutoPartitionByLong autoPartition=new AutoPartitionByLong();
		autoPartition.init4Test(list);
		return autoPartition;
	}

	@Test
	public void test2()  {
		AutoPartitionByLong autoPartition=  getInitPartition();

		Assert.assertEquals(true, null==autoPartition.calculate("0"));

		Assert.assertEquals(true, 0==autoPartition.calculate("1"));

		Assert.assertEquals(true, 0==autoPartition.calculate("100"));

		Assert.assertEquals(true, 1==autoPartition.calculate("101"));

		Assert.assertEquals(true, 1==autoPartition.calculate("200"));

		Assert.assertEquals(true, 2==autoPartition.calculate("201"));

		Assert.assertEquals(true, 2==autoPartition.calculate("400"));

		Assert.assertEquals(true, null ==autoPartition.calculate("401"));
	}

	@Test
	public void test3()  {
		AutoPartitionByLong autoPartition=  getInitPartition();

		String idVal="0";
		Integer[] nodeList = autoPartition.calculateRange(idVal, null, 0);
		Assert.assertEquals(true, nodeList.length == 3 && nodeList[0] != null);

		idVal="1";
		nodeList = autoPartition.calculateRange(idVal, null, 0);
		Assert.assertEquals(true, nodeList.length == 3 && nodeList[0] != null);

		idVal="100";
		nodeList = autoPartition.calculateRange(idVal, null, 0);
		Assert.assertEquals(true, nodeList.length == 3 && nodeList[0] != null);

		idVal="101";
		nodeList = autoPartition.calculateRange(idVal, null, 0);
		Assert.assertEquals(true, nodeList.length == 2 && nodeList[0]  >=  1);

		idVal="200";
		nodeList = autoPartition.calculateRange(idVal, null, 0);
		Assert.assertEquals(true, nodeList.length == 2 && nodeList[0]  >=  1);

		idVal="201";
		nodeList = autoPartition.calculateRange(idVal, null, 0);
		Assert.assertEquals(true, nodeList.length == 1 && 2 == nodeList[0]);

		idVal="400";
		nodeList = autoPartition.calculateRange(idVal, null, 0);
		Assert.assertEquals(true, nodeList.length == 1 && 2 == nodeList[0]);

		nodeList = autoPartition.calculateRange("401", null, 0);
		Assert.assertEquals(true, nodeList.length == 1 && nodeList[0] > 10 );


		idVal="0";
		nodeList = autoPartition.calculateRange(null, idVal, 0);
		Assert.assertEquals(true, nodeList.length == 1 && nodeList[0] > 10 );

		idVal="1";
		nodeList = autoPartition.calculateRange(null, idVal, 0);
		Assert.assertEquals(true, nodeList.length == 1 && nodeList[0] == 0);

		idVal="101";
		nodeList = autoPartition.calculateRange(null, idVal, 0);
		Assert.assertEquals(true, nodeList.length == 2 && nodeList[0] != null);

		idVal="201";
		nodeList = autoPartition.calculateRange(null, idVal, 0);
		Assert.assertEquals(true, nodeList.length == 3 && nodeList[0] != null);

		idVal="401";
		nodeList = autoPartition.calculateRange(null, idVal, 0);
		Assert.assertEquals(true, nodeList.length == 3 && nodeList[0] != null);

		nodeList = autoPartition.calculateRange("0", "0", 0);
		Assert.assertEquals(true, nodeList.length == 1 && nodeList[0].intValue() > 3);

		nodeList = autoPartition.calculateRange("0", "1", 0);
		Assert.assertEquals(true, nodeList.length == 1 && nodeList[0] ==  0);

		nodeList = autoPartition.calculateRange("0", "101", 0);
		Assert.assertEquals(true, nodeList.length == 2 && nodeList[0] != null);

		nodeList = autoPartition.calculateRange("101", "101", 0);
		Assert.assertEquals(true, nodeList.length == 1 && nodeList[0] == 1);

		nodeList = autoPartition.calculateRange("401", "401", 0);
		Assert.assertEquals(true, nodeList.length == 1 && nodeList[0] > 10);

		nodeList = autoPartition.calculateRange("400", "401", 0);
		Assert.assertEquals(true, nodeList.length == 1 && nodeList[0] == 2);

		nodeList = autoPartition.calculateRange("201", "401", 0);
		Assert.assertEquals(true, nodeList.length == 1 && nodeList[0] == 2);

		nodeList = autoPartition.calculateRange("200", "401", 0);
		Assert.assertEquals(true, nodeList.length == 2 && (nodeList[0]  +  nodeList[1])== 3);

		nodeList = autoPartition.calculateRange("200", "200", 0);
		Assert.assertEquals(true, nodeList.length == 1 && nodeList[0] == 1);

		nodeList = autoPartition.calculateRange("101", "200", 0);
		Assert.assertEquals(true, nodeList.length == 1 && nodeList[0] == 1);

		nodeList = autoPartition.calculateRange("100", "200", 0);
		Assert.assertEquals(true, nodeList.length == 2 && (nodeList[0] + nodeList[1]) == 1);

	}

}