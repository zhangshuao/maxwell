package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class RoutingProducer extends AbstractProducer {

	private final Map<String, AbstractProducer> producers;

	public RoutingProducer(MaxwellContext context, Map<String, AbstractProducer> producers) {
		super(context);
		this.producers = producers;
	}

	@Override
	public void push(final RowMap r) throws Exception {
		StringBuilder sb = new StringBuilder();
		String dbKey = sb.append(r.getDatabase()).append(":").toString();
		String dbTableKey = sb.append(r.getTable()).toString();
		sb.setLength(0);
		String tableKey = sb.append(":").append(r.getTable()).toString();

		Optional<AbstractProducer> producer = Stream.of(dbTableKey, dbKey, tableKey).map(producers::get).filter(Objects::nonNull).findFirst();
		if (producer.isPresent()) {
			producer.get().push(r);
		}
	}

}
