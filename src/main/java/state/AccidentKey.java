package state;

public class AccidentKey {
	public Integer xway;
	public Integer lane;
	public Integer dir;
	public Integer seg;
	
	public AccidentKey(Integer xway, Integer lane, Integer dir, Integer seg) {
		this.xway = xway;
		this.lane = lane;
		this.dir = dir;
		this.seg = seg;
	}
	
	public AccidentKey(Integer xway, Integer dir, Integer seg) {
		this.xway = xway;
		this.dir = dir;
		this.seg = seg;
	}
}
