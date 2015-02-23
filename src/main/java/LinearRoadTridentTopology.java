import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import bolt.doInsertPosition;
import bolt.FilterGetDailyExpenditureBolt;
import bolt.FilterGetTravelEstimateBolt;
import bolt.FilterInsertPositionBolt;
import bolt.FilterGetAccountBalanceBolt;
import bolt.FilterTimestampBolt;
import spout.AccidentSpout;
import spout.LinearRoadSpout;
import spout.TimestampSpout;
import state.AccidentDB;
import state.AccidentStateUpdater;
import state.PositionDB;
import state.PositionStateUpdater;
import state.QueryTimestamp;
import state.TimestampDB;
import state.TimestampStateUpdater;
import state.memcached.MemcachedState;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import utils.LinearRoadConstants;

/**
 * Created by cpa on 1/26/15.
 */
public class LinearRoadTridentTopology extends TridentTopology {

    public static void main(String[] args) throws Exception {

        if (args.length > 0) {
            LinearRoadConstants.inputFile = args[0];
        }

        if (args.length > 1) {
            try {
                LinearRoadConstants.memcachedPort = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Argument 2 (" + args[1] + ") must be an integer.");
                System.exit(1);
            }
        }

        Config conf = new Config();
        conf.setDebug( false );
        conf.setMaxSpoutPending( 200 );
        conf.setMaxTaskParallelism(1);
        conf.setFallBackOnJavaSerialization(true);


        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(LinearRoadConstants.streamId, conf, buildTopology( drpc ) );

        System.out.println("\n\n\n\n\n\n\nEXECUTION: " + drpc.execute(LinearRoadConstants.streamId, "") + "\n\n\n\n\n\n\n");

        cluster.shutdown();
        drpc.shutdown();

    }


    public static StormTopology buildTopology( LocalDRPC drpc ) {


        TridentTopology topology = new TridentTopology();
        
        TimestampSpout timestampSpout = new TimestampSpout();
        Fields timestampFields = timestampSpout.getOutputFields();
        
        LinearRoadSpout linearRoadSpout = new LinearRoadSpout();
        Fields linearRoadSpoutFields = linearRoadSpout.getOutputFields();

        StateFactory positionMapState = new MemoryMapState.Factory();

        Stream timestampS = topology.newStream("TimestampSpout", timestampSpout);
        TimestampDB timestampState = (TimestampDB) timestampS.partitionPersist(
        							MemcachedState.opaque(LinearRoadConstants.servers), 
        							new TimestampStateUpdater());
        
//        AccidentSpout accidentSpout = new AccidentSpout();
//        Stream accidentS = topology.newStream("AccidentSpout", accidentSpout);
//        AccidentDB accidentState = (AccidentDB) accidentS.partitionPersist(
//									MemcachedState.opaque(LinearRoadConstants.servers), 
//									new AccidentStateUpdater());
        
        Stream inputS = topology.newStream("LinearRoadSpout", linearRoadSpout);
        Stream insertPositionS = inputS.each(new Fields("flag"), new FilterInsertPositionBolt());
        Stream getAccountBalanceS = inputS.each(new Fields("flag"), new FilterGetAccountBalanceBolt());
        Stream getDailyExpenditureS = inputS.each(new Fields("flag"), new FilterGetDailyExpenditureBolt());
        Stream getTravelEstimateS = inputS.each(new Fields("flag"), new FilterGetTravelEstimateBolt());
        
        doInsertPosition(topology, insertPositionS, timestampState, linearRoadSpoutFields, positionMapState);
        doGetAccountBalance(getAccountBalanceS);
        doGetDailyExpenditure(getDailyExpenditureS);
        doGetTravelEstimate(getTravelEstimateS);
        
        return topology.build();
    }
    
    
    private static void doInsertPosition(TridentTopology topology, Stream insertPositionS, TimestampDB timestampState,
    		Fields linearRoadSpoutFields, StateFactory positionMapState) {

    	// Insert into position
    	PositionDB positionState = (PositionDB) insertPositionS.partitionPersist(
				MemcachedState.opaque(LinearRoadConstants.servers), 
				new PositionStateUpdater());
    	
    	insertPositionS.each(linearRoadSpoutFields, 
    			new doInsertPosition(topology, timestampState, positionState), null);
    	
//    	insertPositionS.stateQuery(timestampState, new Fields("xway"), new QueryTimestamp(), new Fields("tod", "ts"))
//    					.stateQuery(positionState, new Fields("xway", "vid", "ts", "tod"), new QueryPrevPosition(), new Fields("seg"))
//    					.each(new Fields("lane", ""), new FilterLaneBolt());
//
//    	Values vals = linearRoadSpout.getCurrentPositionValues();
//    	int xway = (Integer) vals.get(5);
//    	int part_id = (Integer) vals.get(10);
//    	
//    	timestampsS.each(new Fields("xway"), new FilterTimestampBolt(xway))
//    			   .each(null, new Fields("tod", "ts"));
    }
    
    
    private static void doGetAccountBalance(Stream getAccountBalanceS) {
    	
    }

    
    private static void doGetDailyExpenditure(Stream getDailyExpenditureS) {
    	
    }
    
    
    private static void doGetTravelEstimate(Stream getTravelEstimateS) {
    	
    }
    
    
    public static class MyFunction extends BaseFunction {
        public void execute(TridentTuple tuple, TridentCollector collector) {
            System.out.println("count" + tuple.toString());
//            collector.reportError(new Exception(item.toString()));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            collector.emit(tuple);
        }
    }


}
