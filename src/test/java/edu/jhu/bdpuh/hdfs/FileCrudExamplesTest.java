package edu.jhu.bdpuh.hdfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class FileCrudExamplesTest {
    FileCrudExamples fileCrudExamples;

    @Test
    public void testDefaultFS() {
        String defaultFS = fileCrudExamples.getDefaultFS();
        assertNotNull(defaultFS);
        System.out.printf("fs.defaultFS: %s\n", defaultFS);
    }

    @Test
    public void testHello() throws IOException {
        assertEquals(this.fileCrudExamples.sayHello(), FileCrudExamples.HelloMessage);
    }

    @BeforeEach
    void setUp() throws IOException {
        fileCrudExamples = new FileCrudExamples();
    }
}
