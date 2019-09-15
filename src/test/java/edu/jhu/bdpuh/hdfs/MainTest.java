package edu.jhu.bdpuh.hdfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class MainTest {
    Main main;

     @Test
    public void testHello() throws IOException {
        assertEquals(this.main.sayHello(), Main.HelloMessage);
    }

    @BeforeEach
    void setUp() throws IOException {
        main = new Main();
    }
}
