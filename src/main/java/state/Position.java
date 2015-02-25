package state;

import java.io.Serializable;

/**
 * Created by jdu on 2/12/15.
 */
public class Position implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1409337187611088655L;

    public long time;
    public long vid;
    public long qid;
    public int spd;
    public int xway;
    public int lane;
    public int dir;
    public int seg;
    public int pos;
//    int part_id = Integer.valueOf(values[10]);
    public int segbegin;
    public int segend;
    public int day;
    public long tod;

    public long txnStartTime;

    public Position(long time, long vid, long qid, int spd, int xway,
    				int lane, int dir, int seg, int pos, int segbegin, int segend,
    				int day, long tod) {
        this.time = time;
        this.vid = vid;
        this.qid = qid;
        this.spd =spd;
        this.xway = xway;
        this.lane = lane;
        this.dir = dir;
        this.seg = seg;
        this.pos = pos;
        this.segbegin = segbegin;
        this.segend = segend;
        this.day = day;
        this.tod = tod;
    }
    
    public Position(Position p) {
        this.time = p.time;
        this.vid = p.vid;
        this.qid = p.qid;
        this.spd = p.spd;
        this.xway = p.xway;
        this.lane = p.lane;
        this.dir = p.dir;
        this.seg = p.seg;
        this.pos = p.pos;
        this.segbegin = p.segbegin;
        this.segend = p.segend;
        this.day = p.day;
        this.tod = p.tod;
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
        return "" + this.time + " " +
        this.vid + " " +
        this.qid + " " +
        this.spd + " " +
        this.xway + " " +
        this.lane + " " +
        this.dir + " " +
        this.seg + " " +
        this.pos + " " +
        this.segbegin + " " +
        this.segend + " " +
        this.day + " " +
        this.tod + "\n";
    }

}

