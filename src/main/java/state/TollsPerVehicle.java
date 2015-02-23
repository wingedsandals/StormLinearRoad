package state;

import java.io.Serializable;

/**
 * Created by cpa on 2/12/15.
 */
public class TollsPerVehicle implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1409337187611088655L;

	long vid;
	int tollday;
	int xway;
	int toll;

    public TollsPerVehicle(long vid, int tollday, int xway, int toll) {
    	this.vid = vid;
    	this.tollday = tollday;
        this.xway = xway;
        this.toll = toll;
    }

    public String toString()
    {
        return "" + this.vid + " " + this.tollday + " " + this.xway + " " + this.toll;
    }

}

