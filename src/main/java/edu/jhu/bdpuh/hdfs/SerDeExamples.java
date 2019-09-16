package edu.jhu.bdpuh.hdfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.EOFException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SerDeExamples extends FileCrudExamples {

    // serialize example
    public void writePoints(String filename) throws IOException {
        Point3D record1 = new Point3D(1, 2, 3);
        Point3D record2 = new Point3D(4, 5, 6);
        Point3D record3 = new Point3D(7, 8, 9);
        System.out.printf("Writing 3D points to %s\n", filename);
        FSDataOutputStream fsDataOutputStream = getOutputStream(filename);
        try {
            // Write String records to the file
            record1.write(fsDataOutputStream);
            record2.write(fsDataOutputStream);
            record3.write(fsDataOutputStream);
            fsDataOutputStream.close();
        } catch (IOException ex) {
            Logger.getLogger(SerDeExamples.class.getName()).log(Level.SEVERE, null,ex);
        }
        fsDataOutputStream.close();
    }

    // deserialize example
    public void readPoints(String filename) throws IOException {
        FSDataInputStream fsDataInputStream = getInputStream(filename);
        try {
            // Read Point3DsToFile the file until eof
            System.out.printf("Reading %d bytes from %s\n", fsDataInputStream.available(), filename);
            Point3D point3D = new Point3D();
            // Keep reading until eof
            while (true) {
                point3D.readFields(fsDataInputStream);
                System.out.print (point3D + "\n");
            }
        }
        catch (EOFException e) {
            // All good
        } catch (IOException ex) {
            Logger.getLogger(SerDeExamples.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            fsDataInputStream.close();
        }

    }

    public static void main(String[] args) throws IOException {
        String recordsFilename = "/points.ser";
        SerDeExamples serDeExamples = new SerDeExamples();
        serDeExamples.addConfigs();
        System.out.println(serDeExamples.sayHello());
        System.out.printf("fs.defaultFS: %s\n", serDeExamples.getDefaultFS());
        serDeExamples.openFileSystem();
        System.out.println("**** Serialization Example ****");
        serDeExamples.writePoints(recordsFilename);
        System.out.println("**** Deserialization Example ****");
        serDeExamples.readPoints(recordsFilename);
        System.out.println("Closing the filesytem");
        serDeExamples.closeFileSystem();
    }


}
