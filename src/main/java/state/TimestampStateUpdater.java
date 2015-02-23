package state;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public class TimestampStateUpdater extends BaseStateUpdater {
	
	@Override
	public void updateState(State timestampState, List tuples, TridentCollector collector) {
		List<Integer> xways = new ArrayList<Integer>();
		List<Long> tods = new ArrayList<Long>();
		List<Long> tss = new ArrayList<Long>();
		
		for (Object t : tuples) {
			TridentTuple tt = (TridentTuple)t;
			xways.add(tt.getInteger(0));
			tods.add(tt.getLong(1));
			tss.add(tt.getLong(2));
		}
		
		TimestampDB timestampDB = (TimestampDB) timestampState;
		timestampDB.setXwayBulk(xways, tods, tss);
	}

}
