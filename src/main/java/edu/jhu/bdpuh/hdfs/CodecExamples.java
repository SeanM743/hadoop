package edu.jhu.bdpuh.hdfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.IOException;

public class CodecExamples extends FileCrudExamples {

    // compression example
    public void compressFile(String filename) throws IOException {
        FSDataInputStream fsDataInputStream = getInputStream(filename);
        if(fsDataInputStream != null) {
            FSDataOutputStream fsDataOutputStream = getOutputStream(filename + ".gz");
            CompressionCodec compressionCodec = new GzipCodec();
            CompressionOutputStream compressedOutputStream = null;
            compressedOutputStream = compressionCodec.createOutputStream(fsDataOutputStream);
            // Read from input stream and write to compressed output stream
            System.out.println("Writing compressed output");
            IOUtils.copyBytes(fsDataInputStream, compressedOutputStream,this.conf);
            fsDataInputStream.close();
            fsDataOutputStream.close();
        }
    }

    // decompression example, we assume gzip with .gz extension
    public void decompressFile(String filename) throws IOException {
        FSDataInputStream fsDataInputStream = getInputStream(filename + ".gz");
        if(fsDataInputStream != null) {
            FSDataOutputStream fsDataOutputStream = getOutputStream(filename);
            // Get a Codec and from that create a decompressed input stream
            GzipCodec gzipCodec = new GzipCodec();
            gzipCodec.setConf(this.conf);
            CompressionCodec compressionCodec = gzipCodec;
            CompressionInputStream decompressedInputStream = null;
            decompressedInputStream = compressionCodec.createInputStream(fsDataInputStream);
            // Read from decompressed input stream to output stream
            IOUtils.copyBytes(decompressedInputStream, fsDataOutputStream, this.conf);
            fsDataInputStream.close();
            fsDataOutputStream.close();
        }
    }

    public static void main(String[] args) throws IOException {
        String recordsFilename = "/records.txt";
        CodecExamples codecExamples = new CodecExamples();
        codecExamples.addConfigs();
        System.out.println(codecExamples.sayHello());
        System.out.printf("fs.defaultFS: %s\n", codecExamples.getDefaultFS());
        codecExamples.openFileSystem();
        System.out.println("**** Compression example ****");
        codecExamples.createFile(recordsFilename);
        codecExamples.compressFile(recordsFilename);
        System.out.println("**** Decompression example ****");
        codecExamples.deleteFile(recordsFilename);
        codecExamples.decompressFile(recordsFilename);
        codecExamples.readFile(recordsFilename);
        System.out.println("Closing the filesytem");
        codecExamples.closeFileSystem();
    }

}
