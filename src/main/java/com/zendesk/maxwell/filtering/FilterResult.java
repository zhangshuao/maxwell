package com.zendesk.maxwell.filtering;

import java.util.ArrayList;
import java.util.List;

public class FilterResult {
	public boolean include = true;
	public List<FilterPattern> tablePattens = new ArrayList<>();
	public List<FilterColumnPattern> columnPatterns = new ArrayList<>();
}
