package bolt;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Created by jdu on 2/18/15.
 */
public class FilterGetTravelEstimateBolt extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
//        System.out.println("tup: " + tuple);
        return tuple.getInteger(0) == 4;
    }
}
