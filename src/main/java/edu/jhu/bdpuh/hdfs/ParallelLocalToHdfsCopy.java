package edu.jhu.bdpuh.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.net.URI;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;

//import com.squareup.okhttp.internal.io.FileSystem;

public class ParallelLocalToHdfsCopy {

    public static void main(String[] args) throws IOException {

        System.out.println("BDPUH Assignment 3 - Parallel Local to HDFS copy");
        if (args.length < 1) {
            System.err.println("args: <local-dir> <hdfs-dir> <n-threads>");
            System.exit(-1);
        }
        // check if directory exists on local file system using standard java library
        File f = new File(args[0]);

        if (!f.exists()) {
            System.err.println("File does not exist");
            System.exit(-1);
        }

        String sourcePath = args[0];
        String destPath = args[1];
        int nThreads = Integer.parseInt(args[2]);

        Path dest = new Path(destPath);
        Path src = new Path(sourcePath);

        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/hadoop-3.2.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/hadoop-3.2.0/etc/hadoop/hdfs-site.xml"));

        FileSystem hdfs = FileSystem.get(URI.create(args[1]), conf);
        if (hdfs.exists(dest)){
            System.err.println("Destination directory already exists! " + 
                "Please delete before running program.");
            System.exit(-1);
        }

        hdfs.mkdirs(dest);

        // FileStatus[] fileStatus = hdfs.listStatus(dest);
        // for(FileStatus status : fileStatus){
        //     System.out.println(destPath);
        //     System.out.println(status.getPath().toString());
        // }

        try{
            InputStream is = new BufferedInputStream(new FileInputStream("/home/sean/hd-work/module4-code/pom.xml"));
            OutputStream os = hdfs.create(new Path("/destination/compressed.gz"));
            CompressionCodec compCodec = new GzipCodec();
            CompressionOutputStream compOs = null;
            compOs = compCodec.createOutputStream(os);

            //IOUtils.copyBytes(is, compOs, conf);
        }catch(IOException e){
            e.printStackTrace();
        }
        FileSystem fs = FileSystem.get(new Configuration());
        System.out.println(fs.exists(new Path(sourcePath)));
        System.out.printf("Copying and compressing local files from '%s' to '%s' using %d threads.\n", sourcePath,
                destPath, nThreads);

        // student will complete the code required to do this...

    }
}
