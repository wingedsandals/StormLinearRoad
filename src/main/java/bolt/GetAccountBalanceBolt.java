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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class GetAccountBalanceBolt extends BaseFunction implements Function {
	
	TimestampDB timestampState;
	TollsPerVehicleDB tollsPerVehicleState;

	public GetAccountBalanceBolt(TridentTopology topology, TollsPerVehicleDB tollsPerVehicleState,
			TimestampDB timestampState) {
		this.timestampState = timestampState;
		this.tollsPerVehicleState = tollsPerVehicleState;
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
//        long tod = tuple.getLong(14);
        long tod = time / 60;
        
//        StateFactory mapState = new MemoryMapState.Factory();
//        tollsPerVehicleState.newValuesStream()
//        			.each(new Fields("vid"), new FilterGetVidBolt(vid))
//        			.project(new Fields("toll"))
//        			.persistentAggregate(mapState, new Fields("toll"), new Sum(), new Fields("tollSum"));

        Long tolls = tollsPerVehicleState.sumTolls(new ArrayList<Long>(Arrays.asList(vid)));
        Long updatedTime = timestampState.getMaxTS();

        //hack to return a string back through a VoltTable
        String o = "2,"+time+","+ LinearRoadConstants.EMIT_TIME_STR + "," + updatedTime + "," + qid + "," + tolls;
        
	}


}
