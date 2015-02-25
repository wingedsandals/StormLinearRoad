import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import bolt.CalculateTollsBolt;
import bolt.DetectAccidentBolt;
import bolt.FilterGetVidBolt;
import bolt.GetAccountBalanceBolt;
import bolt.GetDailyExpenditureBolt;
import bolt.InsertPositionBolt;
import bolt.FilterGetDailyExpenditureBolt;
import bolt.FilterGetTravelEstimateBolt;
import bolt.FilterInsertPositionBolt;
import bolt.FilterGetAccountBalanceBolt;
import bolt.FilterTimestampBolt;
import bolt.SplitStreamBolt;
import spout.AccidentSpout;
import spout.LinearRoadSpout;
import spout.SegmentHistorySpout;
import spout.TimestampSpout;
import spout.TollsPerVehicleSpout;
import state.AccidentDB;
import state.AccidentStateUpdater;
import state.InputDB;
import state.InputStateUpdater;
import state.PositionDB;
import state.PositionStateUpdater;
import state.SegmentHistoryDB;
import state.SegmentHistoryStateUpdater;
import state.StartTollCalcDB;
import state.StartTollCalcStateUpdater;
import state.TimestampDB;
import state.TimestampStateUpdater;
import state.TollsPerVehicleDB;
import state.TollsPerVehicleStateUpdater;
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
 * Created by jdu on 2/25/15.
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
        
        TollsPerVehicleSpout tollsPerVehicleSpout = new TollsPerVehicleSpout();
        Stream tollsPerVehicleS = topology.newStream("TollsPerVehicleSpout", tollsPerVehicleSpout);
        TollsPerVehicleDB tollsPerVehicleState = 
        		(TollsPerVehicleDB) tollsPerVehicleS.partitionPersist(
        							MemcachedState.opaque(LinearRoadConstants.servers), 
        							new TollsPerVehicleStateUpdater());
        
        AccidentSpout accidentSpout = new AccidentSpout();
        Stream accidentS = topology.newStream("AccidentSpout", accidentSpout);
        AccidentDB accidentState = (AccidentDB) accidentS.partitionPersist(
									MemcachedState.opaque(LinearRoadConstants.servers), 
									new AccidentStateUpdater());
        
        SegmentHistorySpout segmentHistorySpout = new SegmentHistorySpout();
        Stream segmentHistoryS = topology.newStream("SegmentHistorySpout", segmentHistorySpout);
        SegmentHistoryDB segmentHistoryState = (SegmentHistoryDB) segmentHistoryS.partitionPersist(
				MemcachedState.opaque(LinearRoadConstants.servers), 
				new SegmentHistoryStateUpdater());
        
        Stream inputS = topology.newStream("LinearRoadSpout", linearRoadSpout);
        InputDB inputState = (InputDB) inputS.partitionPersist(
				MemcachedState.opaque(LinearRoadConstants.servers), 
				new InputStateUpdater());
        
        Stream insertPositionS = inputState.newValuesStream().each(new Fields("flag"), new FilterInsertPositionBolt());
        Stream getAccountBalanceS = inputState.newValuesStream().each(new Fields("flag"), new FilterGetAccountBalanceBolt());
        Stream getDailyExpenditureS = inputState.newValuesStream().each(new Fields("flag"), new FilterGetDailyExpenditureBolt());
        Stream getTravelEstimateS = inputState.newValuesStream().each(new Fields("flag"), new FilterGetTravelEstimateBolt());

        doInsertPosition(topology, insertPositionS, timestampState, accidentState, segmentHistoryState, linearRoadSpoutFields, positionMapState);
        doGetAccountBalance(topology, getAccountBalanceS, linearRoadSpoutFields, tollsPerVehicleState, timestampState);
        doGetDailyExpenditure(topology, getDailyExpenditureS, linearRoadSpoutFields, tollsPerVehicleState);
        doGetTravelEstimate(topology, getTravelEstimateS, linearRoadSpoutFields, segmentHistoryState);
        
        return topology.build();
    }
    
    
    private static void doInsertPosition(TridentTopology topology, Stream insertPositionS, 
    		TimestampDB timestampState, AccidentDB accidentState, SegmentHistoryDB segmentHistoryState,
    		Fields linearRoadSpoutFields, StateFactory positionMapState) {

    	// Insert into position
    	PositionDB positionState = (PositionDB) insertPositionS.partitionPersist(
				MemcachedState.opaque(LinearRoadConstants.servers), 
				new PositionStateUpdater());
    	
    	Fields startAccidentCheckFields = new Fields("xway", "tod", "ts");
    	Stream startAccidentCheckS = insertPositionS.each(linearRoadSpoutFields, 
    			new InsertPositionBolt(topology, timestampState, positionState),
    			startAccidentCheckFields);
    	
    	Fields startTollCalcFields = new Fields("xway", "tod", "ts");
    	Stream startTollCalcS = startAccidentCheckS.each(startAccidentCheckFields,
    			new DetectAccidentBolt(topology, positionState),
    			startTollCalcFields);
    	StartTollCalcDB startTollCalcState = (StartTollCalcDB) startTollCalcS.partitionPersist(
    			MemcachedState.opaque(LinearRoadConstants.servers),
    			new StartTollCalcStateUpdater());
    	
    	startTollCalcS.each(startTollCalcFields,
    			new CalculateTollsBolt(topology, positionState, startTollCalcState, 
    					accidentState, segmentHistoryState),
    			null);
    }
    
    
    private static void doGetAccountBalance(TridentTopology topology,
    		Stream getAccountBalanceS,
    		Fields linearRoadSpoutFields,
    		TollsPerVehicleDB tollsPerVehicleState,
    		TimestampDB timestampState) {

    	getAccountBalanceS.each(linearRoadSpoutFields,
    			new GetAccountBalanceBolt(topology, tollsPerVehicleState,
    					timestampState), null);
    }

    
    private static void doGetDailyExpenditure(TridentTopology topology,
    		Stream getDailyExpenditureS,
    		Fields linearRoadSpoutFields,
    		TollsPerVehicleDB tollsPerVehicleState) {
    	
    	getDailyExpenditureS.each(linearRoadSpoutFields,
    			new GetDailyExpenditureBolt(topology, tollsPerVehicleState),
    			null);
    }
    
    
    private static void doGetTravelEstimate(TridentTopology topology,
    		Stream getTravelEstimateS,
    		Fields linearRoadSpoutFields,
    		SegmentHistoryDB segmentHistoryState) {
    	
    	getTravelEstimateS.each(linearRoadSpoutFields,
    			new GetTravelEstimateBolt(topology, segmentHistoryState),
    			null);
    }
    
}
