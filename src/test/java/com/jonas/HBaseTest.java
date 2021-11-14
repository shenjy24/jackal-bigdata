package com.jonas;

import com.jonas.hbase.HBaseAPI;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

public class HBaseTest {

    private final String TEACHER_TABLE = "Teacher";

    @Test
    public void testCreateTable() {
        HBaseAPI.deleteTable(TEACHER_TABLE);
        boolean result = HBaseAPI.createTable(TEACHER_TABLE, Arrays.asList("baseInfo", "schoolInfo"));
        Assert.assertTrue(result);
    }

    @Test
    public void testGetScanner() {
        ResultScanner resultScanner = HBaseAPI.scanTable(TEACHER_TABLE);
        Iterator<Result> resultIterator = resultScanner.iterator();
        while (resultIterator.hasNext()) {
            Result result = resultIterator.next();
            System.out.println(result);
        }
    }

    @Test
    public void testPutRow() {
        boolean result = HBaseAPI.putRow(TEACHER_TABLE, "rowKey1", "baseInfo", "name", "Jonas");
        Assert.assertTrue(result);
    }

    @Test
    public void testGetRow() {
        Result result = HBaseAPI.getRow(TEACHER_TABLE, "rowKey1");
        System.out.println(result);
    }

    @Test
    public void testGetCell() {
        String cellValue = HBaseAPI.getCell(TEACHER_TABLE, "rowKey1", "baseInfo", "name");
        System.out.println(cellValue);
    }
}
