package edu.jhu.bdpuh.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import java.net.URI;

public class ThreadedCopy implements Runnable {

    private String source;
    private String dest;
    private File[] f;
    private Configuration config;

    public ThreadedCopy(File[] files_to_copy, String source, String dest, Configuration conf){
        this.source = source;
        this.dest = dest;
        this.f = files_to_copy;
        this.config = conf;
    }

    public void run(){

        FileSystem hdfs = null;
        FileInputStream is = null;
        OutputStream os = null;
        CompressionCodec compCodec = null;
        CompressionOutputStream compOs = null;

        for (int i = 0; i< f.length; i++){

            try{
                String fname = f[i].getName();
                hdfs = FileSystem.get(URI.create(dest), config);
                is = new FileInputStream(f[i].getAbsolutePath());
                //InputStream is = new BufferedInputStream(fin);
                os = hdfs.create(new Path(dest + '/' + fname), true);
                System.out.println(dest+ '/' + fname + ".gz");
                compCodec = new GzipCodec();                
                compOs = compCodec.createOutputStream(os);
                IOUtils.copyBytes(is, compOs, config);                
                
            }catch(IOException e){
                e.printStackTrace();
            }
            finally{
                try{
                    IOUtils.closeStream(compOs);
                    is.close();
                    //hdfs.close();
                }catch(IOException e){
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Job ran");
    }
}