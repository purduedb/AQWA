package drawing;

import java.awt.Color;
import java.util.HashMap;

public class Line{
	public final int x1; 
	public final int y1;
	public final int x2;
	public final int y2;   
	public final Color color;

	public Line(int x1, int y1, int x2, int y2, Color color) {
		this.x1 = x1;
		this.y1 = y1;
		this.x2 = x2;
		this.y2 = y2;
		this.color = color;
	}

	@Override	
	public String toString() {
		return "Line [x1=" + x1 + ", y1=" + y1 + ", x2=" + x2 + ", y2=" + y2
				+ ", color=" + color + "]";
	}       
	
	public String toJSON(HashMap<Integer,Double> xcmap, HashMap<Integer,Double> ycmap){
		String chex = "#"+Integer.toHexString(color.getRGB()).substring(2);
		if (xcmap.size() > 0 && ycmap.size() > 0)
		{
			return "{\"x1\":"+ xcmap.get(x1)  +", \"y1\":" + ycmap.get(y1) + ", \"x2\":" + xcmap.get(x2) + ", \"y2\":" + ycmap.get(y2)
					+ ", \"color\": \"" + chex + "\"}";
		}
		else {
			
			return toJSON();
		}
	}
	
	public String toJSON(){
		String chex = "#"+Integer.toHexString(color.getRGB()).substring(2);
		return "{\"x1\":"+ x1 +", \"y1\":" + y1 + ", \"x2\":" + x2 + ", \"y2\":" + y2
				+ ", \"color\": \"" + chex + "\"}";
	}
}