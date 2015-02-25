package state;

import java.io.Serializable;

/**
 * Created by jdu on 2/12/15.
 */
public class Accident implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1409337187611088655L;

    public int xway;
    public int lane;
    public int dir;
    public int seg;

    public Accident(int xway, int lane, int dir, int seg) {
        this.xway = xway;
        this.lane = lane;
        this.dir = dir;
        this.seg = seg;
    }
    
    public Accident(Accident a) {
        this.xway = a.xway;
        this.lane = a.lane;
        this.dir = a.dir;
        this.seg = a.seg;
    }

    public String toString()
    {
        return "" + this.xway + " " +
        this.lane + " " +
        this.dir + " " +
        this.seg + "\n";
    }

}

