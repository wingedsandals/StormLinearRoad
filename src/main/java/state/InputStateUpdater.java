package state;

import java.util.ArrayList;
import java.util.List;

import state.Input;
import state.InputDB;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public class InputStateUpdater extends BaseStateUpdater{

	@Override
	public void updateState(State inputState, List tuples, TridentCollector collector) {
		List<Input> inputs = new ArrayList<Input>();
		
		for (Object t : tuples) {
			TridentTuple tuple = (TridentTuple)t;
			int flag = tuple.getInteger(0);
	        long time = tuple.getLong(1);
	        long vid = tuple.getLong(2);
	        long qid = tuple.getLong(3);
	        int spd = tuple.getInteger(4);
	        int xway = tuple.getInteger(5);
	        int lane = tuple.getInteger(6);
	        int dir = tuple.getInteger(7);
	        int seg = tuple.getInteger(8);
	        int pos = tuple.getInteger(9);
	        int part_id = tuple.getInteger(10);
	        int segbegin = tuple.getInteger(11);;
	        int segend = tuple.getInteger(12);
	        int day = tuple.getInteger(13);
	        long tod = tuple.getLong(14);

			Input i = new Input(flag, time, vid, qid, spd, xway,
										lane, dir, seg, pos, part_id, segbegin, segend,
										day, tod);
			inputs.add(i);
		}
		
		InputDB inputDB = (InputDB) inputState;
		inputDB.setInputBulk(inputs);
	}


}
