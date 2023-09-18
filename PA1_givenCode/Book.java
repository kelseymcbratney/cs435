package ngram;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

//IMPORTANT: make sure you understand ALL code and can EXPLAIN it
//IMPORTANT: make sure you understand the role of EACH class
public class Book {
	  private String headerText, bodyText, author, year;
	  private int ngramCount; // #TODO#: specify unigram or bigram

	  public Book(String rawText, int ngramCount) {

				  //#TODO#: initialize class variables based on input
					//#TODO#: separate text with metadata from book text

		  bodyText = formatBook(bodyText);
		  author = parseAuthor(headerText);
		  year = parseYear(headerText);
	  }

	  private String parseAuthor(String headerText) {
		 //#TODO#: extract author (check parseYear() for guidelines)
	  }

	  private String parseYear(String headerText) {
		  Pattern yearPattern = Pattern.compile("#TODO# FILL IN WITH PATTERN TO MATCH");
		  Matcher yearMatcher = yearPattern.matcher(headerText);
		  if(yearMatcher.find()){
			  String yearMatch = yearMatcher.group(1);
			  return "#TODO# SOMETHING"
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
		  if(ngramCount < 2)
			  return "#TODO# BOOK TEXT FOR UNIGRAM - DON'T FORGET PRE-PROCESSING";
				//hint: convert text to lower case and remove punctuations and apostrophies
		  else
			  return "#TODO# BOOK TEXT FOR BIGRAM - DON'T FORGET PRE-PROCESSING";
				//hint: all from previous return + add sentence beginning and end tokens
	  }
}
