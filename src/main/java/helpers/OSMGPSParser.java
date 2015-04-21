package helpers;

public class OSMGPSParser implements CoordinateParser {

  public String[] getCoordinates(String inputStr) {
     return inputStr.split(",");
  }

}
