package state;

import java.io.Serializable;

/**
 * Created by jdu on 2/12/15.
 */
public class Timestamp implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1409337187611088655L;

    public int xway;
    public long tod;
    public long ts;

    public Timestamp(int xway, long tod, long ts) {
        this.xway = xway;
        this.tod = tod;
        this.ts = ts;
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
        return "" + this.xway + " " + this.tod + " " + this.ts + "\n";
    }

}

