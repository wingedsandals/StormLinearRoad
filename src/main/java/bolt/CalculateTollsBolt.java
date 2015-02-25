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
import state.SegmentHistoryStateUpdater;
import state.StartTollCalcDB;
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

public class CalculateTollsBolt extends BaseFunction implements Function {
	
	PositionDB positionState;
	StartTollCalcDB startTollCalcState;
	Stream accidentS;
	AccidentDB accidentState;
	SegmentHistoryDB segmentHistoryState;
	VehicleDB vehicleState;

	public CalculateTollsBolt(TridentTopology topology, PositionDB positionState, 
			StartTollCalcDB startTollCalcState, AccidentDB accidentState,
			SegmentHistoryDB segmentHistoryState) {
		this.positionState = positionState;
		this.startTollCalcState = startTollCalcState;
		this.accidentState = accidentState;
		this.segmentHistoryState = segmentHistoryState;
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
        
        Values minTodXway = startTollCalcState.getMinTodXway();
        assert(minTodXway != null);

        long abstod = (Long) minTodXway.get(0);
    	int xway = (Integer) minTodXway.get(1);
    	
    	int dow = 0;  //(int)(abstod / LinearRoadConstants.MINUTES_PER_DAY) % 7;
    	int tod = (int)(abstod % LinearRoadConstants.MINUTES_PER_DAY);
    	
        List<Values> currAvgSpdInSegment = positionState.getCurrAvgSpdInSegment(xway, abstod);
        
    	int rowCount = currAvgSpdInSegment.size();
    	//assert(rowCount > 0);
    	
    	/////////////////CALCULATE AVG SPEED AND CAR COUNT PER SEGMENT//////////////////////////////
    	int currseg = (Integer) currAvgSpdInSegment.get(0).get(1);
    	int currdir = (Integer) currAvgSpdInSegment.get(0).get(2);
    	int carCount = 0;
    	int totalSpd = 0;
    	boolean[] seenSegs = new boolean[200];
    	
    	for(int i = 0; i < seenSegs.length; i++) {
			seenSegs[i] = false; //denotes all segments with booleans
		}
    	
    	List<SegmentHistory> segmentHistories = new ArrayList<SegmentHistory>();
    	for(int i = 0; i < rowCount; i++)
    	{
    		Values row = currAvgSpdInSegment.get(i);
    		int seg = (Integer)row.get(1);
    		int dir = (Integer)row.get(2);
    		if(seg < 0 || seg > 99 || dir < 0 || dir > 1)
    			continue;
    		
    		seenSegs[seg + (dir*100)] = true;
    		
    		if(currseg != seg || currdir != dir)
    		{
    			// double avgSpd = 0;
    			Integer avgSpd = 0;
    			if(carCount > 0)
    				avgSpd = totalSpd / carCount;
    			
    			//voltQueueSQL(addAvgSpdStmt, xway, part_id, curseg, curdir, tod, avgSpd, carCount);
    			
    			//toll calculation
    			int toll = 0;
    			if(carCount > 50 && avgSpd < 40) {
    				toll = 2 * (carCount - 50)*(carCount - 50);
    			}
    			segmentHistories.add(new SegmentHistory(xway, 0, dow, tod, currseg, currdir, avgSpd, carCount, toll));
//    			voltQueueSQL(addTollStmt, part_id, xway, 0, dow, tod, curseg, curdir, avgSpd, carCount, toll);
    			
    			currseg = seg;
    			currdir = dir;
    			carCount = 0;
    			totalSpd = 0;
    		}
    		carCount++;
    		totalSpd += (Integer)row.get(3);//get speed
    		
    	}
    	int avgSpd = 0;
    	if(carCount > 0)
    		avgSpd = totalSpd / carCount;
		int toll = 0;
		if(carCount > 50 && avgSpd < 40) {
			toll = 2 * (carCount - 50)*(carCount - 50);
		}
		segmentHistories.add(new SegmentHistory(xway, 0, dow, tod, currseg, currdir, avgSpd, carCount, toll));
//		voltQueueSQL(addTollStmt, part_id, xway, 0, dow, tod, curseg, curdir, avgSpd, carCount, toll);
		
		//make sure that we insert a row for segments even if there were no cars
		boolean allSeen = true;
		for(int i = 0; i < seenSegs.length; i++) {
			if(seenSegs[i] == false) {
				allSeen = false;
				segmentHistories.add(new SegmentHistory(xway, 0, dow, tod, i % 100, i / 100, 0, 0, 0));
//				voltQueueSQL(addTollStmt, part_id, xway, 0, dow, tod, i % 100, i / 100, 0, 0, 0);
			}
		}
		if(!allSeen)
			segmentHistoryState.setSegmentHistoryBulk(segmentHistories);
//			voltExecuteSQL();
		
    	
		/////////////////////////////////////////////////////////////////////////////////////////////
		
		////////////////////////////FIND SEGMENTS WITH ACCIDENTS/////////////////////////////////////
		List<Values> accidents = accidentState.getAccidents(xway);
		List<SegmentHistoryKey> segmentHistoryKeys = new ArrayList<SegmentHistoryKey>();
		for(int i = 0; i < accidents.size(); i++)
		{
			Values accident = accidents.get(i);
			int seg = (Integer)accident.get(1);
			int dir = (Integer)accident.get(0);
			for(int j = seg - 4; j <= seg; j++)
			{
				if(j < 0)
					continue;
				
				segmentHistoryKeys.add(new SegmentHistoryKey(xway, 0, tod, j, dir));
//				voltQueueSQL(updateTollStmt, 0, xway, part_id, 0, tod, j, dir);
			}
		}
		segmentHistoryState.updateToll(segmentHistoryKeys);
//		voltExecuteSQL();
		//////////////////////////////////////////////////////////////////////////////////////////////
    	
    	
    	//voltQueueSQL(addTollStmt, part_id, xway, 0, dow, tod, 0, 0, 51, 10, 0);//JOHN REMOVE THIS LATER
//    	voltQueueSQL(DeleteInputStmt, part_id, xway, abstod);
//    	voltExecuteSQL();
        
        //INSERT INTO OUTPUT STREAM
//        collector.emit(new Values(xway, currTOD, currTS));
	}

}
