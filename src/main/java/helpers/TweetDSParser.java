package helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TweetDSParser implements CoordinateParser {

  private static final Log LOG = LogFactory.getLog(TweetDSParser.class);

  public String[] getCoordinates(String inputStr) {
    String[] res = new String[2];
    String[] split = inputStr.split(",");
    LOG.info(inputStr);
    try {
      int lat = (int) (Double.parseDouble(split[2]) * Math.pow(10, 7));
      int lng = (int) (Double.parseDouble(split[3]) * Math.pow(10, 7));
      res[0] = lat + "";
      res[1] = lng + "";
      return res;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return null;
    }

  }

}
