package state;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public class PositionStateUpdater extends BaseStateUpdater{

	@Override
	public void updateState(State positionState, List tuples, TridentCollector collector) {
		List<Position> positions = new ArrayList<Position>();
		
		for (Object t : tuples) {
			TridentTuple tuple = (TridentTuple)t;
	        long time = tuple.getLong(1);
	        long vid = tuple.getLong(2);
	        long qid = tuple.getLong(3);
	        int spd = tuple.getInteger(4);
	        int xway = tuple.getInteger(5);
	        int lane = tuple.getInteger(6);
	        int dir = tuple.getInteger(7);
	        int seg = tuple.getInteger(8);
	        int pos = tuple.getInteger(9);
//	        int part_id = tuple.getInteger(10);
	        int segbegin = tuple.getInteger(11);;
	        int segend = tuple.getInteger(12);
	        int day = tuple.getInteger(13);
//	        long tod = tuple.getLong(14);
	        long tod = time / 60;
			
			Position p = new Position(time, vid, qid, spd, xway,
										lane, dir, seg, pos, segbegin, segend,
										day, tod);
			positions.add(p);
		}
		
		PositionDB positionDB = (PositionDB) positionState;
		positionDB.setXwayBulk(positions);
	}


}
