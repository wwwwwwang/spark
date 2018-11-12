package etlutil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by SNOW on 2017/5/16.
 */
public class GeekUtils {
    private static String AddYear(String date) {
        SimpleDateFormat sDateFormat = new SimpleDateFormat("yyyy");
        String year = sDateFormat.format(new java.util.Date());
        return date + " " + year;
    }

    public static String dateFormatToString(String date, String oldPattern) {
        if (date == null || oldPattern == null)
            return "";
        else if(date.equalsIgnoreCase("") || oldPattern.equalsIgnoreCase("")){
            System.out.println("date or oldPattern is \"\"");
            return "";
        }
        //String newPattern = "yyyy-MM-dd";
        String newPattern = "yyyy-MM-dd'T'HH:mm:ss.SSS+0800";
        SimpleDateFormat sdf2 = new SimpleDateFormat(newPattern);
        String res=date;
        if(oldPattern.equalsIgnoreCase("13") || oldPattern.equalsIgnoreCase("10")){
            //System.out.println("date = " + date);
            long lSysTime1 = Long.parseLong(date);
            if(oldPattern.equalsIgnoreCase("10")){
                lSysTime1=lSysTime1*1000;
            }
            //System.out.println("lSysTime1 = " + lSysTime1);
            java.util.Date d = new Date(lSysTime1);
            res=sdf2.format(d);
        }else{
            SimpleDateFormat sdf1;
            String format;
            String fdate;
            if (oldPattern.contains("y") || oldPattern.contains("Y")) {
                format = oldPattern;
                fdate = date;
            } else {
                format = oldPattern + " yyyy";
                fdate = AddYear(date);
            }
            if (oldPattern.contains("MMM")) {
                sdf1 = new SimpleDateFormat(format, Locale.ENGLISH);
            } else {
                sdf1 = new SimpleDateFormat(format);
            }
            try {
                java.util.Date d = sdf1.parse(fdate);
                res=sdf2.format(d);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        //System.out.println(res);
        return res;
    }
}
