package joins.indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

public class WordLists {
  private static String _commonWords = "WordsMostCommon.txt";
  private static String _englishWords = "WordsEnglish.txt";

  // I happen to know the length of these.....
  private static List<String> _commonList = new ArrayList<String>(2001);
  private static List<String> _englishList = new ArrayList<String>(110000);
  private static List<String> _sourcesList = new ArrayList<String>(200);
  private static List<String> _fewList = new ArrayList<String>(200);

  private static Random _randomGenerator = new Random();

  private static SimpleDateFormat _dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

  private static NumberFormat _numFormat = new DecimalFormat("#,###,###");

  public static void init() throws Exception {
    fillList(new File(_commonWords), _commonList);
    fillList(new File(_englishWords), _englishList);
    for (int idx = 0; idx < 200; ++idx) {
      _sourcesList.add(getCommonWord() + idx);
      _fewList.add(getCommonWord());
    }
  }

  public static String getFew() {
    return _fewList.get(_randomGenerator.nextInt(_fewList.size()));
  }

  public static void getCommonWord(StringBuilder sb) {
    sb.append(_commonList.get(_randomGenerator.nextInt(_commonList.size()))).append(" ");
  }

  public static String getSource() {
    return _sourcesList.get(_randomGenerator.nextInt(_sourcesList.size()));
  }

  public static String getCommonWord() {
    return _commonList.get(_randomGenerator.nextInt(_commonList.size()));
  }


  public static void getEnglishWord(StringBuilder sb) {
    sb.append(_englishList.get(_randomGenerator.nextInt(_englishList.size()))).append(" ");
  }

  public static void fillList(File file, List<String> list) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    String strLine;
    int count = 0;
    while ((strLine = br.readLine()) != null) {
      String[] parts = strLine.split("\\s");
      if (parts.length > 0) {
        list.add(parts[0].replaceAll("\\W", "").trim());
        ++count;
      }
    }
    JoinIndexer.log("Indexed words from " + file.getCanonicalPath() + " " + WordLists.formatNum(count));
  }

  public static long getLong() {
    return _randomGenerator.nextLong();
  }

  public static int getInt(int mod) {
    return _randomGenerator.nextInt(mod);
  }

  // Generate a random date between 1980 and 2011
  public static String getDate() {
    // Note, we're rounding to day here by cleverly adding stuff to the string that solr understands Generating dates
    // to the second, change rounding by appending /HOUR, /DAY etc. at the end.
    Calendar cal = Calendar.getInstance();
    int year = 1980 + _randomGenerator.nextInt(31);
    int month = 1 + _randomGenerator.nextInt(11);
    int day_of_month = 1 + _randomGenerator.nextInt(27);
    int hour_of_day = 0 + _randomGenerator.nextInt(23);
    int minute = 0 + _randomGenerator.nextInt(59);
    int second = 0 + _randomGenerator.nextInt(59);
    cal.set(year, month, day_of_month, hour_of_day, minute, second);
    return _dateFormatter.format(cal.getTime()) + "/DAY";
  }

  public static String formatNum(int num) {
    return _numFormat.format(num);
  }

  public static String formatNum(long num) {
    return _numFormat.format(num);
  }

  public static String genLatLon() {
    Boolean latNeg = _randomGenerator.nextBoolean();
    Boolean lonNeg = _randomGenerator.nextBoolean();

    return String.format("%d.%d,%d.%d",
        WordLists.getInt(89) * ((latNeg) ? -1 : 1),
        WordLists.getInt(10000),
        WordLists.getInt(89) * ((lonNeg) ? -1 : 1),
        WordLists.getInt(10000));

  }
}
