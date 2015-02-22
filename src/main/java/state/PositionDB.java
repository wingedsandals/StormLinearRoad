package state;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.planner.Node;
import storm.trident.state.State;

public class PositionDB extends TridentState implements State {
	
	protected PositionDB(TridentTopology topology, Node node) {
		super(topology, node);
		// TODO Auto-generated constructor stub
	}

	Map<PositionKey, Position> positions = new HashMap<PositionKey, Position>();

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
					new PositionKey(positions.get(i).xway, positions.get(i).time, positions.get(i).tod, positions.get(i).vid), 
					new Position(positions.get(i)));
		}
		
	}
	
	public List<Position> getXwayBulk(List<PositionKey> positionKeys) {
		List<Position> ret = new ArrayList<Position>();
		for (PositionKey positionKey: positionKeys) {
			ret.add(positions.get(positionKey));
		}
		
		return ret;
	}
	
	public void removeXwayBulk(List<Integer> xways, List<Long> tod) {
		// TODO: remove in bulk by xway and tod
		
	}
}
