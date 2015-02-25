package bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import spout.AccidentSpout;
import state.Accident;
import state.AccidentDB;
import state.AccidentKey;
import state.AccidentStateUpdater;
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
import state.memcached.MemcachedState;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
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
import backtype.storm.tuple.Values;

public class DetectAccidentBolt extends BaseFunction implements Function {
	
	PositionDB positionState;
	Stream accidentS;
	AccidentDB accidentState;
	SegmentHistoryDB segmentHistoryState;
	VehicleDB vehicleState;

	public DetectAccidentBolt(TridentTopology topology, PositionDB positionState) {
		this.positionState = positionState;
	}
	
	public void prepare(Map conf, TridentOperationContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
        int xway = tuple.getInteger(0);
        long currTOD = tuple.getLong(1);
        long currTS = tuple.getLong(2);
        
        List<Values>  stoppedCarsView = positionState.getStoppedCars(xway, 
        									LinearRoadConstants.STOPPED_POS_POINTS);
        assert(stoppedCarsView.size() > 0);
        
        HashSet<CarPosition> stoppedCars = new HashSet<CarPosition>();
        HashSet<CarPosition> accidents = new HashSet<CarPosition>();
       
        for(int i = 0; i < stoppedCarsView.size(); i++) {
        	Values v = stoppedCarsView.get(i);
        	int vid = (Integer) v.get(0);
        	int lane = (Integer) v.get(1);
        	int dir = (Integer) v.get(2);
        	int seg = (Integer) v.get(3);
        	int pos = (Integer) v.get(4);
        	int cnt = (Integer) v.get(5);
        	
        	if(cnt < LinearRoadConstants.STOPPED_POS_POINTS)
        		continue;
        	
        	CarPosition cp = new CarPosition(lane, dir, seg, pos);
        	if(stoppedCars.contains(cp)) {
        		accidents.add(cp);
        	}
        	else {
        		stoppedCars.add(cp);
        	}
        }
        
        //report accidents
        accidentState.clearAccidents(xway);
        List<Accident> newAccidents = new ArrayList<Accident>();
        for(CarPosition c : accidents) {
        	Accident a = new Accident(xway, c.lane, c.dir, c. seg);
        	newAccidents.add(a);
        }
        accidentState.setAccidentBulk(newAccidents);
        
        //INSERT INTO OUTPUT STREAM
        collector.emit(new Values(xway, currTOD, currTS));
	}

    private class CarPosition {
    	public int lane;
    	public int dir;
    	public int seg;
    	public int pos;
    	
    	public CarPosition(int l, int d, int s, int p){
    		lane = l;
    		dir = d;
    		seg = s;
    		pos = p;
    	}
    	
    	public boolean equals(CarPosition cp) {
    		if(lane == cp.lane && dir == cp.dir && seg == cp.seg && pos == cp.pos)
    			return true;
    		else
    			return false;
    	}
    }

}
