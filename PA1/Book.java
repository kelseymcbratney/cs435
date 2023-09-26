package ngram;

import java.util.prefs.PreferenceChangeEvent;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//IMPORTANT: make sure you understand ALL code and can EXPLAIN it
//IMPORTANT: make sure you understand the role of EACH class
public class Book {
  private String headerText, bodyText, author, year;
  private int ngramCount;

  public Book(String rawText, int ngramCount) {
    this.ngramCount = ngramCount;

    String pattern = ("\\*\\*\\*");
    String[] raw = rawText.split(pattern, 2);
    headerText = raw[0];
    bodyText = raw[1];

    bodyText = formatBook(bodyText);
    author = parseAuthor(headerText);
    year = parseYear(headerText);
  }

  private String parseAuthor(String headerText) {
    Pattern authorPattern = Pattern.compile("Author: (.*?)");
    Matcher authorMatcher = authorPattern.matcher(headerText);
    if (authorMatcher.find()) {
      String authorMatch = authorMatcher.group(1);
      return authorMatch;
    }

    return "";
  }

  private String parseYear(String headerText) {
    Pattern yearPattern = Pattern.compile("Release Date: (.*?)");
    Matcher yearMatcher = yearPattern.matcher(headerText);
    if (yearMatcher.find()) {
      String yearMatch = yearMatcher.group(1);
      return yearMatch;
    }

    return "";
  }

  public String getBookAuthor() {
    return author;
  }

  public String getBookYear() {
    return year;
  }

  public String getBookHeader() {
    return headerText;
  }

  public String getBookBody() {
    return bodyText;
  }

  private String formatBook(String bookText) {
    String loweredText = bookText.toLowerCase();
    if (ngramCount < 2) {
      String unigramText = loweredText.replaceAll("[^a-z\\s]", "");
      return unigramText;

    } else {
      String cleanedText = loweredText.replaceAll("\\s+", " ").replaceAll("'", "");
      String bigramsText = cleanedText.replaceAll("[.!?]", " _END_ _START_");
      bigramsText = "_START_ " + bigramsText;
      return bigramsText;
    }
  }
}
