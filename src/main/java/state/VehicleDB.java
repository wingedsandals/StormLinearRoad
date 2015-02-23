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

public class VehicleDB extends TridentState implements State {
	
	protected VehicleDB(TridentTopology topology, Node node) {
		super(topology, node);
		// TODO Auto-generated constructor stub
	}

	Map<String, Vehicle> vehicles = new HashMap<String, Vehicle>();

	@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub
		
	}
	
	public void setVehicleBulk(List<Vehicle> vehicles, List<Integer> balances) {
		for (int i = 0; i < vehicles.size(); i++) {
			this.vehicles.put(
					new VehicleKey(vehicles.get(i).vid, vehicles.get(i).xway).toString(), 
					new Vehicle(vehicles.get(i).vid, vehicles.get(i).xway, balances.get(i)));
		}
		
	}
	
	public List<Vehicle> getVehicleBulk(List<VehicleKey> vehicleKeys) {
		List<Vehicle> ret = new ArrayList<Vehicle>();
		for (VehicleKey vehicleKey: vehicleKeys) {
			ret.add(vehicles.get(vehicleKey.toString()));
		}
		
		return ret;
	}
	
}
