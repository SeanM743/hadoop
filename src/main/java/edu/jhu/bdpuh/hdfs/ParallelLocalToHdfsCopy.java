package edu.jhu.bdpuh.hdfs;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.net.URI;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.Arrays;

public class ParallelLocalToHdfsCopy {

    public static void main(String[] args) throws IOException {

        System.out.println("BDPUH Assignment 3 - Parallel Local to HDFS copy");
        if (args.length < 1) {
            System.err.println("args: <local-dir> <hdfs-dir> <n-threads>");
            System.exit(-1);
        }
        // check if directory exists on local file system using standard java library
        File folder = new File(args[0]);

        if (!folder.exists()) {
            System.err.println("Folder does not exist");
            System.exit(-1);
        }

        String sourcePath = args[0];
        String destPath = args[1];
        int nThreads = Integer.parseInt(args[2]);

        Path dest = new Path(destPath);

        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/hadoop-3.2.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/hadoop-3.2.0/etc/hadoop/hdfs-site.xml"));

        FileSystem hdfs = FileSystem.get(URI.create(args[1]), conf);

        // if (hdfs.exists(dest)){
        //     System.err.println("Destination directory already exists! " + 
        //         "Please delete before running program.");
        //     System.exit(-1);
        // }
        if (hdfs.exists(dest)){
            hdfs.delete(dest, true);
        }

        hdfs.mkdirs(dest);

        File f = new File(sourcePath);
        File[] listOfFiles = f.listFiles();
        int num_files = listOfFiles.length;
        int files_per_threads = num_files / nThreads;

        System.out.printf("Copying and compressing local files from '%s' to '%s' using %d threads.\n", sourcePath,
                destPath, nThreads);

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(nThreads);

        for (int i = 0; i < nThreads; i++){
            File[] files_to_copy = Arrays.copyOfRange(listOfFiles, files_per_threads*i, files_per_threads*i + files_per_threads);
            ThreadedCopy tcpy = new ThreadedCopy(files_to_copy, sourcePath, destPath, conf);
            executor.execute(tcpy);
        }
        executor.shutdown();

    }
}





