package com.jonas;

import com.jonas.hadoop.hdfs.FileSystemAPI;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class HdfsTest {

    @Test
    public void testMkdir() {
        FileSystemAPI.mkdir("/user/jonas");
    }

    @Test
    public void testCreateFile() {
        FileSystemAPI.createFile("/user/jonas/test", "hello hadoop!\n你好，hadoop！");
    }

    @Test
    public void testReadFile() {
        String content = FileSystemAPI.readFile("/user/jonas/test");
        System.out.println(content);
    }

    @Test
    public void testRename() {
        boolean result1 = FileSystemAPI.rename("/user/jonas", "/user/jonas12");
        boolean result2 = FileSystemAPI.rename("/user/jonas12", "/user/jonas");
        Assert.assertTrue(result1 & result2);
    }

    @Test
    public void testDelete() {
        boolean result = FileSystemAPI.delete("/user/root");
        Assert.assertTrue(result);
    }

    @Test
    public void testUpload() {
        boolean result = FileSystemAPI.upload("C:\\Users\\n14034\\Desktop\\hdfs", "/user/jonas");
        Assert.assertTrue(result);
    }

    @Test
    public void testListFile() {
        List<FileStatus> fileStatuses = FileSystemAPI.listFileStatus("/user/jonas");
        System.out.println(fileStatuses);
    }

    @Test
    public void testListLocatedFileStatus() {
        List<LocatedFileStatus> fileStatuses = FileSystemAPI.listLocatedFileStatus("/user/jonas", true);
        System.out.println(fileStatuses);
    }

    @Test
    public void testListFileBlockLocation() {
        List<BlockLocation> blockLocations = FileSystemAPI.listFileBlockLocation("/user/jonas/test");
        System.out.println(blockLocations);
    }
}
