package edu.jhu.bdpuh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

import static org.apache.commons.io.IOUtils.EOF;
import static org.apache.commons.io.IOUtils.buffer;

public class Main {
    Configuration conf = new Configuration();
    FileSystem fileSystem = null;
    static final String HelloMessage = "Hello HDFS world!";

    // utility function to open the create the filesystem object from configuration
    void openFileSystem() throws IOException {
        if (this.fileSystem == null) {
            this.fileSystem = FileSystem.get(this.conf);
        }
    }

    // good practice to close the filesystem when no longer needed
    void closeFileSystem() throws IOException {
        if (this.fileSystem != null) {
            this.fileSystem.close();
            this.fileSystem = null;
        }
    }

    // get an output stream, will overwrite by default
    FSDataOutputStream getOutputStream(String filename) throws IOException {
        Path filePath = new Path(filename);
        if(this.fileSystem != null)
            return this.fileSystem.create(filePath);
        return null;
    }

    // get an input stream
    FSDataInputStream getInputStream(String filename) throws IOException {
        Path filePath = new Path(filename);
        if(this.fileSystem != null && this.fileSystem.exists(filePath))
            return this.fileSystem.open(filePath);
        return null;
    }

    // create a file, write data to it, and close it
    public void createFile(String filename) throws IOException {
        // Create a file on HDFS and get the output stream
        FSDataOutputStream fsDataOutputStream = getOutputStream(filename);
        // Write String records to the file
        String record1 = "This is record 001\n";
        String record2 = "This is record 002\n";
        String record3 = "This is record 003\n";
        fsDataOutputStream.write(record1.getBytes());
        fsDataOutputStream.write(record2.getBytes());
        fsDataOutputStream.write(record3.getBytes());
        System.out.println("Created a data file with 4 records");
        // Close the FSDataOutputStream and FileSystem
        fsDataOutputStream.hflush();
        fsDataOutputStream.close();
    }

    // read bytes from string and send to console
    public void readBytes(FSDataInputStream fsDataInputStream) throws IOException {
        System.out.printf("There are %d bytes available to read.\n", fsDataInputStream.available());
        byte[] bytes = new byte[10]; // lets use a buffer size of 20
        int nRead;
        while ((nRead = fsDataInputStream.read(bytes)) > 0) {
            String s = new String(bytes);
            System.out.printf("%s", new String(bytes).substring(0,nRead));
        }
    }

    // read from a file
    public void readFile(String filename) throws IOException {
        // Get a FileSystem Object
        openFileSystem();

       // Open a File for Reading
        FSDataInputStream fsDataInputStream = getInputStream(filename);

        // Read all characters from file
        readBytes(fsDataInputStream);

        // Close the FSDataInputStream
        fsDataInputStream.close();
    }

    // use seek method to re-read a fileinput stream
    public void readFileTwice(String filename) throws IOException {
        // Open a File for Reading
        FSDataInputStream fsDataInputStream = getInputStream(filename);

        // Read them the first time
        System.out.println("Reading from file the first time.");
        readBytes(fsDataInputStream);

        // Rewind and read them again
        System.out.println("Rewinding the input using the seek method");
        fsDataInputStream.seek(0L);
        System.out.println("Reading from file the second time.");
        readBytes(fsDataInputStream);

        // close the inputstream
        fsDataInputStream.close();
    }

    public void appendToFile(String filename) throws IOException {
        // Open a File for append update
        Path fileToAppendUpdate = new Path(filename);
        FSDataOutputStream fSDataOutputStream = null;
        fSDataOutputStream = fileSystem.append(fileToAppendUpdate);
        String record4 = "This is record 004\n";
        String record5 = "This is record 005\n";
        String record6 = "This is record 006\n";
        // Write String records to the file
        fSDataOutputStream.writeBytes(record4);
        fSDataOutputStream.writeBytes(record5);
        fSDataOutputStream.writeBytes(record6);
        // Close the FSDataOutputStream
        fSDataOutputStream.flush();
        fSDataOutputStream.close();
    }

    public void deleteFile(String filename) throws IOException {
        Path filePath = new Path(filename);
        if(this.fileSystem != null && this.fileSystem.exists(filePath)) {
            System.out.println("Deleting file");
            this.fileSystem.delete(filePath, false);
        }
    }

    public void renameFile(String fromFile, String toFile) throws IOException {
        Path fromPath = new Path(fromFile);
        Path toPath = new Path(toFile);
        if(this.fileSystem != null && this.fileSystem.exists(fromPath)) {
            System.out.printf("Renaming %s to %s\n", fromFile, toFile);
            this.fileSystem.rename(fromPath, toPath);
        }
    }

    public void addConfigs() {
        conf.addResource(new Path("/usr/local/hadoop/hadoop-3.2.0/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/hadoop-3.2.0/etc/hadoop/hdfs-site.xml"));
    }

    // get a progressable output stream, will overwrite by default
    FSDataOutputStream getProgressOutputStream(String filename) throws IOException {
        Path filePath = new Path(filename);
        if(this.fileSystem != null)
            return this.fileSystem.create(filePath, new Progressable() {
                @Override
                public void progress() {
                    System.out.print("..");
                }
            });
        return null;
    }

    public void doProgressableWrite(String filename) throws IOException {
        FSDataOutputStream fsDataOutputStream = getProgressOutputStream(filename);
        System.out.printf("Writing lines to %s", filename);
        for(int i = 0; i < 10000; i++) {
            String line = String.format("Line number %08d\n", i);
            fsDataOutputStream.write(line.getBytes());
        }
        fsDataOutputStream.close();
        System.out.println("done");
    }

    public String getDefaultFS() { return this.conf.get("fs.defaultFS"); }

    public String sayHello() {
        return HelloMessage;
    } 

    public static void main(String[] args) throws IOException {
        String recordsFilename = "/records.txt";
        String metricsFilename = "/metrics.txt";
        Main main = new Main();
        main.addConfigs();
        System.out.println(main.sayHello());
        System.out.printf("fs.defaultFS: %s\n", main.getDefaultFS());
        main.openFileSystem();
        System.out.println("**** Create a file example ****");
        main.createFile(recordsFilename);
        System.out.println("**** Read a file example ****");
        main.readFile(recordsFilename);
        System.out.println("**** Read a file twice example ****");
        main.readFileTwice(recordsFilename);
        System.out.println("**** Append to a file example ****");
        main.appendToFile(recordsFilename);
        main.readFile(recordsFilename);
        System.out.println("**** Delete file example ****");
        main.deleteFile(recordsFilename);
        System.out.println("**** Rename file example ****");
        main.createFile(recordsFilename);
        main.renameFile(recordsFilename, metricsFilename);
        System.out.println("**** Progressable write example ****");
        main.doProgressableWrite(recordsFilename);
        System.out.println("Closing the filesytem");
        main.closeFileSystem();
    }
}
