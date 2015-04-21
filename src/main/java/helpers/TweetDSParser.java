package helpers;

public class TweetDSParser implements CoordinateParser {

  public String[] getCoordinates(String inputStr) {
    String [] res = new String[2];
    String [] split = inputStr.split(",");
    int lat = (int) (Double.parseDouble(split[2])*Math.pow(10, 7));
    int lng = (int) (Double.parseDouble(split[3])*Math.pow(10, 7));
    res[0] = lat+"";
    res[1] = lng+"";
    return res;
  }

}
