package edu.jhu.bdpuh.hdfs;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point3D implements WritableComparable<Point3D> {
    public float x, y, z;
    public Point3D(float x, float y, float z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }
    public Point3D() {
        this(0.0f, 0.0f, 0.0f);
    }
    public void write(DataOutput out) throws IOException {
        out.writeFloat(x);
        out.writeFloat(y);
        out.writeFloat(z);
    }
    public void readFields(DataInput in) throws IOException {
        x = in.readFloat();
        y = in.readFloat();
        z = in.readFloat();
    }

    public String toString() {
        return "("
                + Float.toString(x) + ", "+ Float.toString(y) + ", "
        + Float.toString(z) + ")";
    }
    /** return the Euclidean distance from (0, 0, 0) */
    public float distanceFromOrigin() {
        return (float)Math.sqrt(x*x + y*y + z*z);
    }
    public int compareTo(Point3D other) {
        float myDistance = distanceFromOrigin();
        float otherDistance = other.distanceFromOrigin(); return Float.compare(myDistance, otherDistance);
    }

    public boolean equals(Object o) {
        if (!(o instanceof Point3D)) {
            return false;
        }
        Point3D other = (Point3D)o;
        return this.x == other.x && this.y == other.y
                && this.z == other.z;
    }

    public int hashCode() {
        return Float.floatToIntBits(x)
                ^ Float.floatToIntBits(y)
                ^ Float.floatToIntBits(z);
    }
}
