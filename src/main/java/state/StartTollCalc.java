package state;

import java.io.Serializable;

/**
 * Created by jdu on 2/12/15.
 */
public class StartTollCalc implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1409337187611088655L;

    public int xway;
    public long tod;
    public long ts;

    public StartTollCalc(int xway, long tod2, long ts2) {
        this.xway = xway;
        this.tod = tod2;
        this.ts = ts2;
    }
    
    public String toString()
    {
        return "" + this.xway + " " + this.tod + " " + this.ts;
    }

}

