package bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import state.Accident;
import state.AccidentDB;
import state.AccidentKey;
import state.Position;
import state.PositionDB;
import state.PositionKey;
import state.SegmentHistory;
import state.SegmentHistoryDB;
import state.SegmentHistoryKey;
import state.Timestamp;
import state.TimestampDB;
import state.Vehicle;
import state.VehicleDB;
import state.VehicleKey;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import utils.LinearRoadConstants;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class doInsertPosition extends BaseFunction implements Function {
	
	TimestampDB timestampState;
	PositionDB positionState;
	Stream accidentS;
	AccidentDB accidentState;
	SegmentHistoryDB segmentHistoryState;
	VehicleDB vehicleState;

	public doInsertPosition(TimestampDB timestampState, PositionDB positionState, 
			Stream accidentS, AccidentDB accidentState) {
		this.timestampState = timestampState;
		this.positionState = positionState;
		this.accidentS = accidentS;
		this.accidentState = accidentState;
	}
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub
        long flag = tuple.getLong(0);
        long time = tuple.getLong(1);
        long vid = tuple.getLong(2);
        long qid = tuple.getLong(3);
        int spd = tuple.getInteger(4);
        int xway = tuple.getInteger(5);
        int lane = tuple.getInteger(6);
        int dir = tuple.getInteger(7);
        int seg = tuple.getInteger(8);
        int pos = tuple.getInteger(9);
//        int part_id = tuple.getInteger(10);
        int segbegin = tuple.getInteger(11);;
        int segend = tuple.getInteger(12);
        int day = tuple.getInteger(13);
        long tod = tuple.getLong(14);
		
        List<Timestamp> timestamps = 
        		timestampState.getXwayBulk(new ArrayList<Integer>(Arrays.asList(xway)));
        Integer timestampsCnt = timestamps.size();
        Long currTOD = timestamps.get(0).tod;
        Long currTS = timestamps.get(0).ts;
        
        PositionKey positionKey = new PositionKey(xway, time-30, (time-30)/60, vid);
        List<Position> prevPositions =
        		positionState.getXwayBulk(new ArrayList<PositionKey>(Arrays.asList(positionKey)));
        
        if (lane == LinearRoadConstants.ENTRANCE_LANE || prevPositions.size() != 0) {
        	int prevSeg = -1;
        	if (prevPositions.size() > 0) {
        		prevSeg = prevPositions.get(0).seg;
        	}
        	if (prevSeg != seg) {
        		List<AccidentKey> accidentKeys = new ArrayList<AccidentKey>();
        		for (int i = 0; i <= 4; i++) {
        			AccidentKey accidentKey = new AccidentKey(xway, dir, seg+i);
        			accidentKeys.add(accidentKey);
        		}
        		// TODO: search with less key elements
        		List<Accident> accidents = 
        				accidentState.getAccidentBulk(accidentKeys);
        		
        		List<SegmentHistoryKey> segmentHistoryKeys = new ArrayList<SegmentHistoryKey>();
        		SegmentHistoryKey segmentHistoryKey = new SegmentHistoryKey(xway, dir, seg, 0, currTOD-1);
        		segmentHistoryKeys.add(segmentHistoryKey);
        		List<SegmentHistory> segmentHistories =
        				segmentHistoryState.getSegmentHistoryBulk(segmentHistoryKeys);        		
        		
        		List<VehicleKey> vehicleKeys = new ArrayList<VehicleKey>();
        		VehicleKey vehicleKey = new VehicleKey(vid, xway);
        		vehicleKeys.add(vehicleKey);
        		List<Vehicle> vehicles =
        				vehicleState.getVehicleBulk(vehicleKeys);
        		
        		int toll = 0;
        		if (segmentHistories.size() > 0) {
        			toll = segmentHistories.get(0).toll;
        		}
        		
        		if (vehicles.size() < 1) {
        			// insert new vehicle info
        			Vehicle newV = new Vehicle(vid, toll, xway);
        			List<Vehicle> newVs = new ArrayList<Vehicle>(Arrays.asList(newV));
        			List<Integer> balances = new ArrayList<Integer>(Arrays.asList(toll));
        			vehicleState.setVehicleBulk(newVs, balances);
        		} else {
        			// update vehicle info
        			toll += vehicles.get(0).balancePerXway;
        			List<Integer> balances = new ArrayList<Integer>(Arrays.asList(toll));
        			vehicleState.setVehicleBulk(vehicles, balances);
        		}
        	}
        }
        
        if (currTOD < tod) {
        	long newMinTOD = currTOD - LinearRoadConstants.NUM_MINUTES_HISTORY;
        	long currCount = accidentState.getAccidentCount();
        	// TODO: add the accident to the stream
        	
        	// Update current timestamp
        	List<Integer> xways = new ArrayList<Integer>(Arrays.asList(xway));
        	List<Long> tods = new ArrayList<Long>(Arrays.asList(tod));
        	List<Long> tss = new ArrayList<Long>(Arrays.asList(time));
        	timestampState.setXwayBulk(xways, tods, tss);
        	
        	// Remove old position
        	positionState.removeXwayBulk(xways, tods);
        }
	}


}
