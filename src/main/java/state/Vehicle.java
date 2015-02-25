package state;

import java.io.Serializable;

/**
 * Created by jdu on 2/12/15.
 */
public class Vehicle implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1409337187611088655L;

    public long vid;
    public int balancePerXway;
    public int xway;

    public Vehicle(long vid, int balancePerXway, int xway) {
    	this.vid = vid;
    	this.balancePerXway = balancePerXway;
        this.xway = xway;
    }
    
    public Vehicle(Vehicle v) {
    	this.vid = v.vid;
    	this.balancePerXway = v.balancePerXway;
        this.xway = v.xway;
    }

    public String toString()
    {
        return "" + this.vid + " " +
        		this.balancePerXway + " " +
        		this.xway + "\n";
    }

}

