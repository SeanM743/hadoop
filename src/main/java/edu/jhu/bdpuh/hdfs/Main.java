package edu.jhu.bdpuh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Main {
    Configuration conf = new Configuration();
    static final String HelloMessage = "Hello HDFS world!";

    public void addCoreSite() {
        conf.addResource(new Path("/usr/local/hadoop/hadoop-3.2.0/etc/hadoop/core-site.xml"));
    }

    public String getDefaultFS() { return this.conf.get("fs.defaultFS"); }

    public String sayHello() {
        return HelloMessage;
    } 

    public static void main(String[] args) {
        Main main = new Main();
        main.addCoreSite();
        System.out.println(main.sayHello());
        System.out.printf("fs.defaultFS: %s\n", main.getDefaultFS());
    }
}
