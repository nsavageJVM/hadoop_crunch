package org.eduonix.etl;

import java.io.Serializable;

public class LineItem implements Serializable {


    String x;
    String y;
    String z;



    public LineItem(String x, String y, String z) {
        super();
        this.x = x;
        this.y = y;
        this.z = z;
    }



    public LineItem() {
        super();
    }


    public String getX() {
        return x;
    }

    public void setX(String x) {
        this.x = x;
    }

    public String getY() {
        return y;
    }

    public void setY(String y) {
        this.y = y;
    }

    public String getZ() {
        return z;
    }

    public void setZ(String z) {
        this.z = z;
    }


    @Override
    public String toString() {
        return "{x: "+ x + " , y:  " + y + ", z: " + z + "} "+'\n';
    }
}
