package edu.jhu.bdpuh.hdfs;

import org.apache.hadoop.conf.Configuration;

public class Main {
    Configuration conf = new Configuration();
    static final String HelloMessage = "Hello HDFS world!";

    public String getDefaultFS() { return this.conf.get("fs.defaultFS"); }

    public String sayHello() {
        return HelloMessage;
    } 

    public static void main(String[] args) {
        Main main = new Main();
        System.out.println(main.sayHello());
        System.out.printf("fs.defaultFS: %s\n", main.getDefaultFS());
    }
}
