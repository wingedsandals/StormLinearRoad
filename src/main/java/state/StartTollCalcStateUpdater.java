package state;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public class StartTollCalcStateUpdater extends BaseStateUpdater {
	
	@Override
	public void updateState(State startTollCalcState, List tuples, TridentCollector collector) {
		List<StartTollCalc> startTollCalcs = new ArrayList<StartTollCalc>();
		
		for (Object t : tuples) {
			TridentTuple tt = (TridentTuple)t;
			
	        int xway = tt.getInteger(0);
	        int tod = tt.getInteger(1);
	        int ts = tt.getInteger(2);
			
			startTollCalcs.add(new StartTollCalc(xway, tod, ts));
		}
		
		StartTollCalcDB startTollCalcDB = (StartTollCalcDB) startTollCalcState;
		startTollCalcDB.setStartTollCalcBulk(startTollCalcs);
	}

}
