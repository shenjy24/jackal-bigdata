package com.jonas.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 操作HDFS的接口
 */
public class FileSystemAPI {

    private static final String HDFS_URL = "hdfs://192.168.157.128:8020";
    private static final String HDFS_USER = "jonas";
    private static FileSystem fileSystem;

    static {
        try {
            Configuration configuration = new Configuration();
            // 本示例启动的是单节点的 Hadoop,所以副本系数设置为 1,默认值为 3
            configuration.set("dfs.replication", "1");
            configuration.set("fs.defaultFS", HDFS_URL);
            fileSystem = FileSystem.get(new URI(HDFS_URL), configuration, HDFS_USER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建目录
     *
     * @param dir 目录路径
     */
    public static boolean mkdir(String dir) {
        try {
            return fileSystem.mkdirs(new Path(dir));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 创建指定权限的目录
     *
     * @param dir        目录路径
     * @param permission 权限
     */
    public static boolean mkdir(String dir, FsPermission permission) {
        try {
            return fileSystem.mkdirs(new Path(dir), permission);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 创建文件，并写入内容。如果文件存在则覆盖
     *
     * @param filePath 文件路径
     * @param content  文件内容
     */
    public static boolean createFile(String filePath, String content) {
        try (FSDataOutputStream out = fileSystem.create(new Path(filePath), true, 4096)) {
            out.write(content.getBytes(StandardCharsets.UTF_8));
            // 强制将缓冲区中内容刷出
            out.flush();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 读取文件内容
     *
     * @param filePath 文件路径
     * @return 文件内容
     */
    public static String readFile(String filePath) {
        try (FSDataInputStream inputStream = fileSystem.open(new Path(filePath))) {
            return inputStreamToString(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    private static String inputStreamToString(FSDataInputStream inputStream) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            StringBuilder builder = new StringBuilder();
            String str = "";
            while ((str = reader.readLine()) != null) {
                builder.append(str).append("\n");
            }
            return builder.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * 重命名文件或目录
     *
     * @param oldPath 旧路径
     * @param newPath 新路径
     */
    public static boolean rename(String oldPath, String newPath) {
        try {
            Path op = new Path(oldPath);
            Path np = new Path(newPath);
            return fileSystem.rename(op, np);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 删除目录或文件
     *
     * @param path 路径
     */
    public static boolean delete(String path) {
        return delete(path, true);
    }

    /**
     * 删除目录或文件
     *
     * @param path      路径
     * @param recursive 是否递归删除
     */
    public static boolean delete(String path, boolean recursive) {
        /*
         *  第二个参数代表是否递归删除
         *  如果 path 是一个目录且递归删除为 true, 则删除该目录及其中所有文件;
         *  如果 path 是一个目录但递归删除为 false,则会则抛出异常。
         */
        try {
            return fileSystem.delete(new Path(path), recursive);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 上传本地文件到HDFS
     * 如果指定的是目录，则会把目录及其中的文件都复制到指定目录下
     *
     * @param localPath  本地路径
     * @param remotePath 远程路径
     */
    public static boolean upload(String localPath, String remotePath) {
        try {
            Path lp = new Path(localPath);
            Path rp = new Path(remotePath);
            fileSystem.copyFromLocalFile(lp, rp);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 上传大文件，可展示上传进度
     *
     * @param localFilePath 本地文件目录
     * @param remotePath    远程路径
     */
    public static boolean uploadBigFile(String localFilePath, String remotePath) {
        File file = new File(localFilePath);
        if (!file.isFile() || !file.exists()) {
            return false;
        }
        InputStream in = null;
        FSDataOutputStream out = null;
        try {
            final float fileSize = file.length();
            in = new BufferedInputStream(new FileInputStream(file));
            out = fileSystem.create(new Path(remotePath), new Progressable() {
                long fileCount = 0;

                @Override
                public void progress() {
                    fileCount++;
                    // progress 方法每上传大约 64KB 的数据后就会被调用一次
                    System.out.println("上传进度：" + (fileCount * 64 * 1024 / fileSize) * 100 + " %");
                }
            });
            IOUtils.copyBytes(in, out, 4096);

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (null != in) {
                    in.close();
                }
                if (null != out) {
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 从HDFS上下载文件
     *
     * @param remotePath 远程路径
     * @param localPath  本地路径
     */
    public static boolean download(String remotePath, String localPath) {
        Path src = new Path(remotePath);
        Path dst = new Path(localPath);
        /*
         * 第一个参数控制下载完成后是否删除源文件,默认是 true,即删除;
         * 最后一个参数表示是否将 RawLocalFileSystem 用作本地文件系统;
         * RawLocalFileSystem 默认为 false,通常情况下可以不设置,
         * 但如果你在执行时候抛出 NullPointerException 异常,则代表你的文件系统与程序可能存在不兼容的情况 (window 下常见),
         * 此时可以将 RawLocalFileSystem 设置为 true
         */
        try {
            fileSystem.copyToLocalFile(false, src, dst, true);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 查看指定目录下所有文件的信息
     *
     * @param path 路径
     */
    public static List<FileStatus> listFileStatus(String path) {
        try {
            FileStatus[] statuses = fileSystem.listStatus(new Path(path));
            if (0 == statuses.length) {
                return new ArrayList<>();
            }
            return new ArrayList<>(Arrays.asList(statuses));
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    /**
     * 查看指定目录下所有文件的信息
     *
     * @param path      路径
     * @param recursive 是否递归
     */
    public static List<LocatedFileStatus> listLocatedFileStatus(String path, boolean recursive) {
        try {
            RemoteIterator<LocatedFileStatus> statuses = fileSystem.listFiles(new Path(path), recursive);
            List<LocatedFileStatus> fileStatuses = new ArrayList<>();
            while (statuses.hasNext()) {
                fileStatuses.add(statuses.next());
            }
            return fileStatuses;
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    /**
     * 查看文件的块信息
     *
     * @param filePath 路径
     * @return 块输出信息有三个值，分别是文件的起始偏移量 (offset)，文件大小 (length)，块所在的主机名 (hosts)。
     */
    public static List<BlockLocation> listFileBlockLocation(String filePath) {
        try {
            FileStatus fileStatus = fileSystem.getFileStatus(new Path(filePath));
            BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            return new ArrayList<>(Arrays.asList(blockLocations));
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }
}
