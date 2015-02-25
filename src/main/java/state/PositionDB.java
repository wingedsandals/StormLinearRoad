package state;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Values;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.planner.Node;
import storm.trident.state.State;

public class PositionDB extends TridentState implements State {
	
	protected PositionDB(TridentTopology topology, Node node) {
		super(topology, node);
		// TODO Auto-generated constructor stub
	}

	Map<String, Position> positions = new HashMap<String, Position>();

	@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub
		
	}
	
	public void setXwayBulk(List<Position> positions) {
		for (int i = 0; i < positions.size(); i++) {
			this.positions.put(
					new PositionKey(positions.get(i).xway, positions.get(i).time, positions.get(i).tod, positions.get(i).vid).toString(), 
					new Position(positions.get(i)));
		}
		
	}
	
	public List<Position> getXwayBulk(List<PositionKey> positionKeys) {
		List<Position> ret = new ArrayList<Position>();
		for (PositionKey positionKey: positionKeys) {
			ret.add(positions.get(positionKey.toString()));
		}
		
		return ret;
	}
	
	
	// Linear search - bad! We should come with better solutions. MemcachedState maybe?
	public void removeXwayBulk(List<Integer> xways, List<Long> tod) {
		Iterator<Map.Entry<String,Position>> iter = positions.entrySet().iterator();
		while (iter.hasNext()) {
		    Map.Entry<String,Position> entry = iter.next();
		    if(xways.contains(entry.getKey()) && 
		    		entry.getValue().tod < tod.get(xways.indexOf(entry.getValue().xway))){
		        iter.remove();
		    }
		}		
	}
	
	// TODO: count on xway, vid, lane, dir, seg, pos
	public List<Values> getAggregate() {
		List ret = new ArrayList();
		// TODO:
		Values v = new Values();
		ret.add(v);
		
		return ret;
	}
	
	// TODO: filter getAggregate where points >= count of getAggregate
	public List<Values> getStoppedCars(int xway, int points) {
		List ret = new ArrayList();
		return ret;
	}

	public List<Values> getCurrAvgSpdInSegment(int xway, long abstod) {
		List ret = new ArrayList();
		
		return ret;
	}
	
}
