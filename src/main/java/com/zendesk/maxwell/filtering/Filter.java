package com.zendesk.maxwell.filtering;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Filter {
	static final Logger LOGGER = LoggerFactory.getLogger(Filter.class);

	private final List<FilterPattern> tablePatterns;
	private final List<FilterColumnPattern> columnPatterns;

	public Filter() {
		this.tablePatterns = new ArrayList<>();
		this.columnPatterns = new ArrayList<>();
	}

	public Filter(String filterString) throws InvalidFilterException {
		this();

		FilterParser filterParser = new FilterParser(filterString);
		filterParser.parse();
		tablePatterns.addAll(filterParser.getTablePatterns());
		columnPatterns.addAll(filterParser.getColumnPatterns());
	}

	public void addRule(String filterString) throws InvalidFilterException {
		this.tablePatterns.addAll(new FilterParser(filterString).parse());
	}

	public List<FilterPattern> getRules() {
		return new ArrayList<>(this.tablePatterns);
	}


	public FilterResult includes(String database, String table) {
		FilterResult match = new FilterResult();

		for ( FilterPattern p : tablePatterns)
			p.match(database, table, match);

		return match;
	}

	public boolean includes(FilterResult result, Map<String, Object> data) {
		for (FilterColumnPattern p : result.columnPatterns)
			p.matchValue(data, result);

		return result.include;
	}

	public void associateColumnPatterns(String database, String table, FilterResult result) {
		for (FilterColumnPattern p : columnPatterns) {
			if (p.couldIncludeColumn(database, table)) {
				result.columnPatterns.add(p);
			}
		}
	}

	public boolean isTableBlacklisted(String database, String table) {
		FilterResult match = new FilterResult();

		for ( FilterPattern p : tablePatterns) {
			if ( p.getType() == FilterPatternType.BLACKLIST )
				p.match(database, table, match);
		}

		return !match.include;
	}

	public boolean isDatabaseBlacklisted(String database) {
		for ( FilterPattern p : tablePatterns) {
			if (p.getType() == FilterPatternType.BLACKLIST &&
				p.getDatabasePattern().matcher(database).find() &&
				p.getTablePattern().toString().equals(""))
				return true;
		}

		return false;
	}

	public static boolean isSystemBlacklisted(String databaseName, String tableName) {
		return "mysql".equals(databaseName) &&
			("ha_health_check".equals(tableName) || StringUtils.startsWith(tableName, "rds_heartbeat"));
	}

	public static FilterResult includes(Filter filter, String database, String table) {
		if (filter == null) {
			return null;
		} else {
			return filter.includes(database, table);
		}
	}

	public static boolean includes(Filter filter, FilterResult result, Map<String, Object> data) {
		if (filter == null) {
			return true;
		} else {
			return filter.includes(result, data);
		}
	}

	public static Filter fromOldFormat(
		String includeDatabases,
		String excludeDatabases,
		String includeTables,
		String excludeTables,
		String blacklistDatabases,
		String blacklistTables,
		String includeValues
	) throws InvalidFilterException {
		ArrayList<String> filterRules = new ArrayList<>();

		if ( blacklistDatabases != null ) {
			for ( String s : blacklistDatabases.split(",") )
				filterRules.add("blacklist: " + s + ".*");
		}

		if ( blacklistTables != null ) {
			for (String s : blacklistTables.split(","))
				filterRules.add("blacklist: *." + s);
		}

		/* any include in old-filters is actually exclude *.* */
		if ( includeDatabases != null || includeTables != null ) {
			filterRules.add("exclude: *.*");
		}

		if ( includeDatabases != null ) {
			for ( String s : includeDatabases.split(",") )
				filterRules.add("include: " + s + ".*");

		}

		if ( excludeDatabases != null ) {
			for (String s : excludeDatabases.split(","))
				filterRules.add("exclude: " + s + ".*");
		}

		if ( includeTables != null ) {
			for ( String s : includeTables.split(",") )
				filterRules.add("include: *." + s);
		}

		if ( excludeTables != null ) {
			for ( String s : excludeTables.split(",") )
				filterRules.add("exclude: *." + s);
		}

		if (includeValues != null && !"".equals(includeValues)) {
			for (String s : includeValues.split(",")) {
				String[] columnAndValue = s.split("=");
				filterRules.add("exclude: *.*." + columnAndValue[0] + "=*");
				filterRules.add("include: *.*." + columnAndValue[0] + "=" + columnAndValue[1]);
			}
		}

		String filterRulesAsString = String.join(", ", filterRules);
		LOGGER.warn("using exclude/include/includeColumns is deprecated.  Please update your configuration to use: ");
		LOGGER.warn("filter = \"" + filterRulesAsString + "\"");

		return new Filter(filterRulesAsString);
	}
}
