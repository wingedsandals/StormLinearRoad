package bolt;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Created by jdu on 2/18/15.
 */
public class FilterGetVidBolt extends BaseFilter {
	private long vid;
	
	public FilterGetVidBolt(long vid2) {
		this.vid = vid2;
	}
	
    @Override
    public boolean isKeep(TridentTuple tuple) {
//        System.out.println("tup: " + tuple);
        return tuple.getLong(0) == vid;
    }
}
