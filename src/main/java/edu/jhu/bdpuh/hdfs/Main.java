package edu.jhu.bdpuh.hdfs;

public class Main {
    static final String HelloMessage = "Hello Maven world!";

    public String sayHello() {
        return HelloMessage;
    } 

    public static void main(String[] args) {
        Main main = new Main();
        System.out.println(main.sayHello());
    }
}
