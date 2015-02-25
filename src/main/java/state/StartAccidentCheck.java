package state;

import java.io.Serializable;

/**
 * Created by jdu on 2/12/15.
 */
public class StartAccidentCheck implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1409337187611088655L;

    public int xway;
    public long tod;
    public long ts;

    public StartAccidentCheck(int xway, long tod, long ts) {
        this.xway = xway;
        this.tod = tod;
        this.ts = ts;
    }
    
    public String toString()
    {
        return "" + this.xway + " " +
        this.tod + " " +
        this.ts + "\n";
    }

}

