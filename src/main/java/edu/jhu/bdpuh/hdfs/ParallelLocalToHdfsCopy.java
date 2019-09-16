package edu.jhu.bdpuh.hdfs;

public class ParallelLocalToHdfsCopy {

    public static void main(String[] args) {

        System.out.println("BDPUH Assignment 3 - Parallel Local to HDFS copy");
        if(args.length < 3) {
            System.err.println("args: <local-dir> <hdfs-dir> <n-threads>");
            System.exit(-1);
        }
        String sourcePath = args[0];
        String destPath = args[1];
        int nThreads = Integer.parseInt(args[2]);
        System.out.printf("Copying and compressing local files from '%s' to '%s' using %d threads.\n", sourcePath, destPath, nThreads);

        // student will complete the code required to do this...
    }

}
