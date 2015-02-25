package state;

public class StartTollCalcKey {
	public Long tod;
	public Integer xway;
	
	public StartTollCalcKey(Long tod, Integer xway) {
		this.tod = tod;
		this.xway = xway;
	}
	
	public String toString() {
		return String.valueOf(this.tod) + " " + String.valueOf(this.xway);
	}
}
