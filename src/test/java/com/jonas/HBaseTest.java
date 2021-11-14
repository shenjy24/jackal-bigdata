package com.jonas;

import com.jonas.hbase.HBaseAPI;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Test;

import java.util.Iterator;

public class HBaseTest {

    @Test
    public void testGetScanner() {
        ResultScanner resultScanner = HBaseAPI.getScanner("Student");
        Iterator<Result> resultIterator = resultScanner.iterator();
        while (resultIterator.hasNext()) {
            Result result = resultIterator.next();
            System.out.println(result);
        }
    }
}
