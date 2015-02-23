package state;

public class VehicleKey {
	public long vid;
	public Integer xway;
	
	public VehicleKey(long vid2, Integer xway) {
		this.vid = vid2;
		this.xway = xway;
	}
	
	public String toString() {
		return String.valueOf(this.vid) + " " +
				String.valueOf(this.xway);
	}
}
