package helpers;

public class Tuple {

	public Location location;
	
	public String tupleData;
	public double distance;
		
	public Tuple(double x, double y, String data) {
		this.tupleData = data;
		location = new Location(x, y);
	}
	
	public void setDistance(FocalPoint pointLocation) {
		double sumOfSquares = 0;
		sumOfSquares += Math.pow(this.location.xCoord - pointLocation.x, 2);
		sumOfSquares += Math.pow(this.location.yCoord - pointLocation.y, 2);				
		this.distance = Math.sqrt(sumOfSquares);
	}
	
	public class Location {
		
		public double xCoord;
		public double yCoord;
		
		public Location(double x, double y) {
			xCoord = x;
			yCoord = y;
		}
				
	}
	
}
