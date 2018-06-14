package com.zendesk.maxwell.filtering;

import com.amazonaws.util.StringInputStream;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StreamTokenizer;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static java.io.StreamTokenizer.TT_EOF;
import static java.io.StreamTokenizer.TT_WORD;

public class FilterParser {
	private StreamTokenizer tokenizer;
	private InputStreamReader inputStream;
	private final String input;
	private List<FilterPattern> tablePatterns;
	private List<FilterColumnPattern> columnPatterns;
	private boolean hasNext = true;

	public FilterParser(String input) {
		this.input = input;
		tablePatterns = new ArrayList<>();
		columnPatterns = new ArrayList<>();
	}

	public List<FilterPattern> parse() throws InvalidFilterException {
		try {
			this.inputStream = new InputStreamReader(new StringInputStream(input));
		} catch ( UnsupportedEncodingException e ) {
			throw new InvalidFilterException(e.getMessage());
		}

		this.tokenizer = new StreamTokenizer(inputStream);
		try {
			doParse();
			return consolidate();
		} catch ( IOException e ) {
			throw new InvalidFilterException(e.getMessage());
		}
	}

	private void doParse() throws IOException {
		tokenizer.ordinaryChar('.');
		tokenizer.ordinaryChar('/');
		tokenizer.wordChars('_', '_');

		tokenizer.ordinaryChars('0', '9');
		tokenizer.wordChars('0', '9');

		tokenizer.quoteChar('`');
		tokenizer.quoteChar('\'');
		tokenizer.quoteChar('"');

		tokenizer.nextToken();

		while ( hasNext )
			parseFilterPattern();
	}

	private void skipToken(char token) throws IOException {
		if ( tokenizer.ttype != token ) {
			throw new IOException("Expected '" + token + "', saw: " + tokenizer.toString());
		}
		tokenizer.nextToken();
	}

	private void parseFilterPattern() throws IOException {
		FilterPatternType type;

		if ( tokenizer.ttype == TT_EOF ) {
			hasNext = false;
			return;
		}

		if ( tokenizer.ttype != TT_WORD )
			throw new IOException("expected [include, exclude, blacklist] in filter definition.");

		switch(tokenizer.sval.toLowerCase()) {
			case "include":
				type = FilterPatternType.INCLUDE;
				break;
			case "exclude":
				type = FilterPatternType.EXCLUDE;
				break;
			case "blacklist":
				type = FilterPatternType.BLACKLIST;
				break;
			default:
				throw new IOException("Unknown filter keyword: " + tokenizer.sval);
		}
		tokenizer.nextToken();

		skipToken(':');
		Pattern dbPattern = parsePattern();
		skipToken('.');
		Pattern tablePattern = parsePattern();

		if ( tokenizer.ttype == '.' ) {
			// column-value filter
			tokenizer.nextToken();

			if ( tokenizer.ttype != TT_WORD )
				throw new IOException("expected column name, got" + tokenizer.nextToken());

			String columnName = tokenizer.sval;
			tokenizer.nextToken();

			skipToken('=');
			Pattern valuePattern = parsePattern();
			columnPatterns.add(new FilterColumnPattern(type, dbPattern, tablePattern, columnName, valuePattern));
		} else {
			tablePatterns.add(new FilterPattern(type, dbPattern, tablePattern));
		}

		if ( tokenizer.ttype == ',' ) {
			tokenizer.nextToken();
		}
	}

	private Pattern parsePattern() throws IOException {
		Pattern pattern;
		switch ( tokenizer.ttype ) {
			case '/':
				pattern = Pattern.compile(parseRegexp());
				break;
			case '*':
				pattern = Pattern.compile("");
				break;
			case TT_WORD:
			case '`':
			case '\'':
			case '"':
				pattern = Pattern.compile("^" + tokenizer.sval + "$");
				break;
			default:
				throw new IOException();
		}
		tokenizer.nextToken();
		return pattern;
	}

	private String parseRegexp() throws IOException {
		char ch, lastChar = 0;
		String s = "";
		while ( true ) {
			ch = (char) inputStream.read();
			if ( ch == '/' && lastChar != '\\' )
				break;

			s += ch;
			lastChar = ch;
		}
		return s;
	}

	private List<FilterPattern> consolidate() {
		// seems quite hard or even impossible to consolidate here
		// if it's possible, then make `FilterPattern` have an Optional<FilterColumnPattern>
		return new ArrayList<>();
	}

	public List<FilterPattern> getTablePatterns() {
		return tablePatterns;
	}

	public List<FilterColumnPattern> getColumnPatterns() {
		return columnPatterns;
	}
}
