package org.myorg.processing;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Util {
  public static Timestamp convertStringToTimestamp(String format, String strDate) {
    try {
      DateFormat formatter = new SimpleDateFormat(format);
       // you can change format of date
      Date date = formatter.parse(strDate);
      Timestamp timeStampDate = new Timestamp(date.getTime());

      return timeStampDate;
    } catch (ParseException e) {
      System.out.println("Exception :" + e);
      return null;
    }
  }

  public static int TimestampDiffInSeconds(Timestamp oldTs, Timestamp newTs) {
    long diffInMS = newTs.getTime() - oldTs.getTime();
    return ((int) diffInMS / 1000);
  }

  public static double HaversineDistance(double lat1, double lat2,
    double lon1, double lon2) {

    final int R = 6371; // Radius of the earth

    double latDistance = Math.toRadians(lat2 - lat1);
    double lonDistance = Math.toRadians(lon2 - lon1);
    double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
            + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
            * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    double distance = R * c * 1000; // convert to meters

    return distance;
  }
}