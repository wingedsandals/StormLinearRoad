package state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.planner.Node;
import storm.trident.state.State;

public class TimestampDB extends TridentState implements State {
	
	protected TimestampDB(TridentTopology topology, Node node) {
		super(topology, node);
		// TODO Auto-generated constructor stub
	}

	Map<Integer, Timestamp> timestamps = new HashMap<Integer, Timestamp>();
	
	@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub
		
	}
	
	public void setXwayBulk(List<Integer> xways, List<Long> tods, List<Long> tss) {
		for (int i = 0; i < xways.size(); i++) {
			timestamps.put(xways.get(i), new Timestamp(xways.get(i), tods.get(i), tss.get(i)));
		}
		
	}
	
	public List<Timestamp> getXwayBulk(List<Integer> xways) {
		List<Timestamp> ret = new ArrayList<Timestamp>();
		for (Integer xway : xways) {
			ret.add(timestamps.get(xway));
		}
		
		return ret;
	}
	
	public Long getMaxTS() {
		Long ts = Long.MIN_VALUE;
		for (Integer k : timestamps.keySet()) {
			if (timestamps.get(k).ts > ts) {
				ts = timestamps.get(k).ts;
			}
		}
		return ts;
	}
	
}
