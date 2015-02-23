package state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.planner.Node;
import storm.trident.state.State;

public class TollsPerVehicleDB extends TridentState implements State {
	
	Map<Long, TollsPerVehicle> tollsPerVehicles = new HashMap<Long, TollsPerVehicle>();
	
	protected TollsPerVehicleDB(TridentTopology topology, Node node) {
		super(topology, node);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub
		
	}
	
	public void setTollsPerVehiclesBulk(List tollsPerVehicles) {

	}
	
	public List<TollsPerVehicle> getTollsBulk(List<Long> vids) {
		List<TollsPerVehicle> ret = new ArrayList<TollsPerVehicle>();
		for (Long vid : vids) {
			ret.add(tollsPerVehicles.get(vid));
		}
		return ret;
	}
	
	public Long sumTolls(List<Long> vids) {
		Long ret = (long) 0;
		List<TollsPerVehicle> tolls = getTollsBulk(vids);
		for (TollsPerVehicle toll : tolls) {
			ret += toll.toll;
		}
		return ret;
	}
	

}
