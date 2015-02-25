package state;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public class AccidentStateUpdater extends BaseStateUpdater {

	@Override
	public void updateState(State accidentState, List tuples, TridentCollector collector) {
		List<Integer> xways = new ArrayList<Integer>();
		List<Integer> lanes = new ArrayList<Integer>();
		List<Integer> dirs = new ArrayList<Integer>();
		List<Integer> segs = new ArrayList<Integer>();
		
		List<Accident> accidents = new ArrayList<Accident>();
		for (Object t : tuples) {
			TridentTuple tt = (TridentTuple)t;
			Accident accident = new Accident(tt.getInteger(0), tt.getInteger(1),
											tt.getInteger(2), tt.getInteger(3));
			accidents.add(accident);
		}
		
		AccidentDB accidentDB = (AccidentDB) accidentState;
		accidentDB.setAccidentBulk(accidents);
	}

}
