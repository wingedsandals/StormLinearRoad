package bolt;

import java.util.ArrayList;
import java.util.Arrays;
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
import state.TollsPerVehicleDB;
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
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import utils.LinearRoadConstants;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitStreamBolt extends BaseFunction implements Function {
	
	Fields fields = new Fields("time", "vid", "qid", "spd", "xway", "lane", 
			"dir", "seg", "pos", "segbegin", "segend", "day", "tod");
	
	public SplitStreamBolt(TridentTopology topology) {
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
//        int part_id = tuple.getInteger(10);
        int segbegin = tuple.getInteger(11);;
        int segend = tuple.getInteger(12);
        int day = tuple.getInteger(13);
        long tod = tuple.getLong(14);

        Values positionValues = new Values(time, vid, qid,
        		spd, xway, lane, dir, seg, pos,
//        		part_id, 
        		segbegin, segend, day, tod);
        OutputCollector outputCollector = new OutputCollector((IOutputCollector) collector);
        switch(flag) {
        	case 0: outputCollector.emit("insertPositionS", positionValues);
        			break;
        	case 2: outputCollector.emit("getAccountBalanceS", positionValues);
        			break;
        	case 3: outputCollector.emit("getDailyExpenditureS", positionValues);
        			break;
        	case 4: outputCollector.emit("getTravelEstimateS", positionValues);
        			break;
        	default: break;
        }
        
	}
	
	public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
	    outputFieldsDeclarer.declareStream("insertPositionS", fields);
	    outputFieldsDeclarer.declareStream("getAccountBalanceS", fields);
	    outputFieldsDeclarer.declareStream("getDailyExpenditureS", fields);
	    outputFieldsDeclarer.declareStream("getTravelEstimateS", fields);
	}


}
