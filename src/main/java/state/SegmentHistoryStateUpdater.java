package state;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public class SegmentHistoryStateUpdater extends BaseStateUpdater {
	
	@Override
	public void updateState(State segmentHistoryState, List tuples, TridentCollector collector) {
		List<SegmentHistory> segmentHistories = new ArrayList<SegmentHistory>();
		
		for (Object t : tuples) {
			TridentTuple tt = (TridentTuple)t;
			
	        int absday = tt.getInteger(0);
	        int tod = tt.getInteger(1);
	        int xway = tt.getInteger(2);
	        int dir = tt.getInteger(3);
	        int seg = tt.getInteger(4);
	        int lav = tt.getInteger(5);
	        int cnt = tt.getInteger(6);
	        int toll = tt.getInteger(7);
	        int dow = tt.getInteger(8);
			
			segmentHistories.add(new SegmentHistory(absday, tod, xway,
					dir, seg, lav, cnt, toll, dow));
		}
		
		SegmentHistoryDB segmentHistoryDB = (SegmentHistoryDB) segmentHistoryState;
		segmentHistoryDB.setSegmentHistoryBulk(segmentHistories);
	}

}
