package state;

import java.io.Serializable;

/**
 * Created by jdu on 2/12/15.
 */
public class Input implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1409337187611088655L;

    public int flag;
    public long time;
    public long vid;
    public long qid;
    public int spd;
    public int xway;
    public int lane;
    public int dir;
    public int seg;
    public int pos;
    public int part_id;
    public int segbegin;
    public int segend;
    public int day;
    public long tod;

    public long txnStartTime;

    public Input(int flag, long time, long vid, long qid, int spd, int xway,
    				int lane, int dir, int seg, int pos, int part_id, 
    				int segbegin, int segend, int day, long tod) {
    	this.flag = flag;
        this.time = time;
        this.vid = vid;
        this.qid = qid;
        this.spd =spd;
        this.xway = xway;
        this.lane = lane;
        this.dir = dir;
        this.seg = seg;
        this.pos = pos;
        this.part_id = part_id;
        this.segbegin = segbegin;
        this.segend = segend;
        this.day = day;
        this.tod = tod;
    }
    
    public Input(Input i) {
    	this.flag = i.flag;
        this.time = i.time;
        this.vid = i.vid;
        this.qid = i.qid;
        this.spd = i.spd;
        this.xway = i.xway;
        this.lane = i.lane;
        this.dir = i.dir;
        this.seg = i.seg;
        this.pos = i.pos;
        this.part_id = i.part_id;
        this.segbegin = i.segbegin;
        this.segend = i.segend;
        this.day = i.day;
        this.tod = i.tod;
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

    public String toString() {
        return this.flag + " " + 
        		this.time + " " +
        		this.vid + " " +
        		this.qid + " " +
        		this.spd + " " +
        		this.xway + " " +
        		this.lane + " " +
        		this.dir + " " +
        		this.seg + " " +
        		this.pos + " " +
        		this.part_id + " " +
        		this.segbegin + " " +
        		this.segend + " " +
        		this.day + " " +
        		this.tod + "\n";
    }

}

