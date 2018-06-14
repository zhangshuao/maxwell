package com.zendesk.maxwell.filtering;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;

public class FilterColumnPattern {
	private static final Logger LOGGER = LoggerFactory.getLogger(FilterColumnPattern.class);
	protected final FilterPatternType type;
	private final Pattern dbPattern, tablePattern;
	private final String columnName;
	private final Pattern columnPattern;
	private final boolean columnPatternIsNull;

	public FilterColumnPattern(
		FilterPatternType type,
		Pattern dbPattern,
		Pattern tablePattern,
		String columnName,
		Pattern columnPattern
	) {
		this.type = type;
		this.dbPattern = dbPattern;
		this.tablePattern = tablePattern;
		this.columnName = columnName;
		this.columnPattern = columnPattern;
		this.columnPatternIsNull = "^null$".equals(columnPattern.toString().toLowerCase());
	}

	public void matchValue(Map<String, Object> data, FilterResult match) {
		boolean applyFilter = false;
		if ( data.containsKey(columnName) ) {
			Object value = data.get(columnName);

			if ( columnPatternIsNull ) {
				// null or "null" (string) or "NULL" (string) is expected
				if (value == null || "null".equals(value) || "NULL".equals(value)) {
					applyFilter = true;
				}
			} else if ( value == null ) {
				// wildcards match the null value
				if ( columnPattern.pattern().length() == 0 )
					applyFilter = true;
			} else {
				if ( columnPattern.matcher(value.toString()).find() ) {
					match.include = (this.type == FilterPatternType.INCLUDE);
				}
			}
		}

		if ( applyFilter )
			match.include = (this.type == FilterPatternType.INCLUDE);
	}

	public boolean couldIncludeColumn(String database, String table) {
		return type == FilterPatternType.INCLUDE && appliesTo(database, table);
	}

	// TODO
	private boolean appliesTo(String database, String table) {
		return (database == null || dbPattern.matcher(database).find())
				&& (table == null || tablePattern.matcher(table).find());
	}

	// TODO
	private String patternToString(Pattern p) {
		String s = p.pattern();

		if ( s.equals("") ) {
			return "*";
		} else if ( s.startsWith("^") && s.endsWith("$") ) {
			return s.substring(1, s.length() - 1);
		} else {
			return "/" + s + "/";
		}
	}

	@Override
	public String toString() {
		String filterString = super.toString();
		return filterString + "." + columnName + "=" + patternToString(columnPattern);
	}
}
