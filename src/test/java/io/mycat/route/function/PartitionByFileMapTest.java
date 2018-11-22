package io.mycat.route.function;

import io.mycat.sqlengine.mpp.RangeValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class PartitionByFileMapTest {
    @Test
    public void test()  {
        PartitionByFileMap partition=new PartitionByFileMap();
        HashMap app2Partition = new HashMap<Object, Integer>();

//		 10000=0
//		 10010=1
//		 10020=2
        app2Partition.put(10000, Integer.valueOf(0));
        app2Partition.put(10010, Integer.valueOf(1));
        app2Partition.put(10020, Integer.valueOf(2));
        partition.init4Test(app2Partition);

        // key > "00010"
        Integer[] result = partition.calculateRange("00010", null, RangeValue.NE);
        Assert.assertEquals(true, 3 == result.length);
        Assert.assertEquals(true, result[0] != null);
        // key > "10000"
        result = partition.calculateRange("10000", null, RangeValue.NE);
        Assert.assertEquals(true, 2 == result.length);
        Assert.assertEquals(true, result[0] != null);
        // key > "10000"
        result = partition.calculateRange("10000", null, RangeValue.NN);
        Assert.assertEquals(true, 2 == result.length);
        Assert.assertEquals(true, result[0] != null);
        // key >= "10000"
        result = partition.calculateRange("10000", null, RangeValue.EE);
        Assert.assertEquals(true, 3 == result.length);
        Assert.assertEquals(true, result[0] != null);
        // key >= "10000"
        result = partition.calculateRange("10000", null, RangeValue.EN);
        Assert.assertEquals(true, 3 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key > "10010"
        result = partition.calculateRange("10010", null, RangeValue.NE);
        Assert.assertEquals(true, 1 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key >= "10010"
        result = partition.calculateRange("10010", null, RangeValue.EE);
        Assert.assertEquals(true, 2 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key > "10020"
        result = partition.calculateRange("10020", null, RangeValue.NE);
        Assert.assertEquals(true, 0 == result.length);

        // key >= "10020"
        result = partition.calculateRange("10020", null, RangeValue.EE);
        Assert.assertEquals(true, 1 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key < "10000"
        result = partition.calculateRange(null, "10000", RangeValue.EN);
        Assert.assertEquals(true, 0 == result.length);

        // key < "10000"
        result = partition.calculateRange(null, "10000", RangeValue.NN);
        Assert.assertEquals(true, 0 == result.length);

        // key <= "10000"
        result = partition.calculateRange(null, "10000", RangeValue.EE);
        Assert.assertEquals(true, 1 == result.length);
        // key <= "10000"
        result = partition.calculateRange(null, "10000", RangeValue.NE);
        Assert.assertEquals(true, 1 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key < "10010"
        result = partition.calculateRange(null, "10010", RangeValue.EN);
        Assert.assertEquals(true, 1 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key < "10010"
        result = partition.calculateRange(null, "10010", RangeValue.NN);
        Assert.assertEquals(true, 1 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key <= "10010"
        result = partition.calculateRange(null, "10010", RangeValue.EE);
        Assert.assertEquals(true, 2 == result.length);
        Assert.assertEquals(true, result[0] != null);
        // key <= "10010"
        result = partition.calculateRange(null, "10010", RangeValue.NE);
        Assert.assertEquals(true, 2 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key < "10020"
        result = partition.calculateRange(null, "10020", RangeValue.EN);
        Assert.assertEquals(true, 2 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key < "10020"
        result = partition.calculateRange(null, "10020", RangeValue.EN);
        Assert.assertEquals(true, 2 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key <= "10020"
        result = partition.calculateRange(null, "10020", RangeValue.EE);
        Assert.assertEquals(true, 3 == result.length);
        Assert.assertEquals(true, result[0] != null);
        // key <= "10020"
        result = partition.calculateRange(null, "10020", RangeValue.NE);
        Assert.assertEquals(true, 3 == result.length);
        Assert.assertEquals(true, result[0] != null);


        // key < "10030"
        result = partition.calculateRange(null, "10030", RangeValue.EN);
        Assert.assertEquals(true, 3 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key < "10030"
        result = partition.calculateRange(null, "10030", RangeValue.NN);
        Assert.assertEquals(true, 3 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key <= "10030"
        result = partition.calculateRange(null, "10030", RangeValue.EE);
        Assert.assertEquals(true, 3 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key <= "10030"
        result = partition.calculateRange(null, "10030", RangeValue.NE);
        Assert.assertEquals(true, 3 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key > "10010" and key < "10020"
        result = partition.calculateRange("10010", "10020", RangeValue.NN);
        Assert.assertEquals(true, 0 == result.length);

        // key >= "10010" and key < "10020"
        result = partition.calculateRange("10010", "10020", RangeValue.EN);
        Assert.assertEquals(true, 1 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key > "10010" and key <= "10020"
        result = partition.calculateRange("10010", "10020", RangeValue.NE);
        Assert.assertEquals(true, 1 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key > "10020" and key <= "10020"
        result = partition.calculateRange("10020", "10020", RangeValue.NE);
        Assert.assertEquals(true, 0 == result.length);

        // key >= "10020" and key <= "10020"
        result = partition.calculateRange("10020", "10020", RangeValue.EE);
        Assert.assertEquals(true, 1 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key >= "10020" and key < "10030"
        result = partition.calculateRange("10020", "10030", RangeValue.EN);
        Assert.assertEquals(true, 1 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key > "00010" and key < "10000"
        result = partition.calculateRange("00010", "10000", RangeValue.NN);
        Assert.assertEquals(true, 0 == result.length);

        // key > "00010" and key <= "10000"
        result = partition.calculateRange("00010", "10000", RangeValue.NE);
        Assert.assertEquals(true, 1 == result.length);
        Assert.assertEquals(true, result[0] != null);

        // key > "00010" and key < "10010"
        result = partition.calculateRange("00010", "10010", RangeValue.NN);
        Assert.assertEquals(true, 1 == result.length);
        Assert.assertEquals(true, result[0] != null);


    }

}
