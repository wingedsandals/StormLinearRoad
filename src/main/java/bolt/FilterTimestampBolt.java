package bolt;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import utils.LinearRoadConstants;

/**
 * Created by jdu on 2/18/15.
 */
public class FilterTimestampBolt extends BaseFilter {
	int xway;
	
	public FilterTimestampBolt(int xway) {
		this.xway = xway;
	}
	
    @Override
    public boolean isKeep(TridentTuple tuple) {
//        System.out.println("tup: " + tuple);
        return tuple.getInteger(0).equals(xway);
    }
}