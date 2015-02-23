package spout;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.TridentTopology;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

public class TollsPerVehicleSpout implements IBatchSpout {

	@Override
	public void open(Map conf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void ack(long batchId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getOutputFields() {
	    return new Fields("vid", "tollday", "xway", "tolls");
	}

}
