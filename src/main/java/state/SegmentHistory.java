package state;

import java.io.Serializable;

/**
 * Created by jdu on 2/12/15.
 */
public class SegmentHistory implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1409337187611088655L;

    public int absday;
    public int tod;
    public int xway;
    public int dir;
    public int seg;
    public int lav;
    public int cnt;
    public int toll;
    public int dow;

    public SegmentHistory(int absday, int tod, int xway, int dir, int seg, int lav, 
    		int cnt, int toll, int dow) {
        this.absday = absday;
        this.tod = tod;
        this.xway = xway;
        this.dir = dir;
        this.seg = seg;
        this.lav = lav;
        this.cnt = cnt;
        this.toll = toll;
        this.dow = dow;
    }
    
    public SegmentHistory(SegmentHistory sg) {
        this.absday = sg.absday;
        this.tod = sg.tod;
        this.xway = sg.xway;
        this.dir = sg.dir;
        this.seg = sg.seg;
        this.lav = sg.lav;
        this.cnt = sg.cnt;
        this.toll = sg.toll;
        this.dow = sg.dow;
    }

//    private void writeObject(ObjectOutputStream s) throws IOException {
//        s.writeLong(this.voteId);
//        s.writeInt(this.contestantNumber);
//        s.writeLong(this.phoneNumber);
//        s.writeInt(this.timestamp);
//        s.writeLong(this.txnStartTime);
//    }
//
//    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
//        this.voteId = s.readLong();
//        this.contestantNumber = s.readInt();
//        this.phoneNumber = s.readLong();
//        this.timestamp = s.readInt();
//        this.txnStartTime = s.readLong();
//    }
//
//    public void debug() {
//        System.out.println("call : " + this.voteId + "-" + this.phoneNumber + "-" + this.contestantNumber + "-" + this.timestamp);
//    }
//
//    public String getString()
//    {
//        return "" + this.voteId + " " + this.phoneNumber + " " + this.contestantNumber + " " + this.timestamp  + " " + txnStartTime + "\n";
//    }

    public String toString()
    {
        return "" + this.absday + " " +
                this.tod + " " +
                this.xway + " " +
                this.dir + " " +
                this.seg + " " +
                this.lav + " " +
                this.cnt + " " +
                this.toll + " " +
                this.dow;
    }

}

