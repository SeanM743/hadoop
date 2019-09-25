package edu.jhu.bdpuh.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class DirCrudExamples extends FileCrudExamples {

    // create a directory example
    public void createDirectory(String dirname) throws IOException {
        Path dir = new Path(dirname);
        // Create a Directory
        boolean status = this.fileSystem.mkdirs(dir);
        System.out.println("Created dir successfully: " + status);
    }

    // list directory example
    public void listDirectory(String rootDir) throws IOException {
        Path dirToRead = new Path(rootDir);
        FileStatus[] fileStatuses = null;
        fileStatuses = this.fileSystem.listStatus(dirToRead);
        // Read records records from the file
        for (int i = 0; i < fileStatuses.length; i++) {
            System.out.println(fileStatuses[i].getPath().getName() + " " + (fileStatuses[i].isDir() ? "dir" : "file"));
        }
        System.out.println("Read dir successfully");
    }

    // globbing example
    public void listByGlob(String globPattern) throws IOException {
        Path glob = new Path(globPattern);
        FileStatus[] fileStatuses = null;
        fileStatuses = fileSystem.globStatus(glob);
        for (FileStatus file : fileStatuses) {
            System.out.println(file.getPath().getName());
        }
        System.out.println("Match File patterns successfully");
    }

    // delete directory
    public void deleteDirectory(String dirname) throws IOException {
        Path dirToDelete = new Path(dirname);
        boolean recursively = true;
        boolean status = fileSystem.delete(dirToDelete, recursively);
        System.out.println("Deleted dir successfully: " + status);
    }

    public static void main(String[] args) throws IOException {
        DirCrudExamples dirCrudExamples = new DirCrudExamples();
        dirCrudExamples.addConfigs();
        System.out.println(dirCrudExamples.sayHello());
        System.out.printf("fs.defaultFS: %s\n", dirCrudExamples.getDefaultFS());
        dirCrudExamples.openFileSystem();
        System.out.println("**** Create a directory example ****");
        dirCrudExamples.createDirectory("/data/dir/subdir/subsubdir");
        System.out.println("**** List a directory example ****");
        dirCrudExamples.listDirectory("/");
        System.out.println("**** Delete a directory example ****");
        dirCrudExamples.deleteDirectory("/data");
        System.out.println("**** Globbing example ****");
        dirCrudExamples.listByGlob("/???");
        System.out.println("Closing the filesytem");
        dirCrudExamples.closeFileSystem();
    }

}
