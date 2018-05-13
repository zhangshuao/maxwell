package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.MaxwellDiagnostic;
import com.zendesk.maxwell.monitoring.MaxwellProducerMetrics;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.StoppableTask;

public abstract class AbstractProducer {
	protected final MaxwellContext context;
	protected final MaxwellOutputConfig outputConfig;
	protected final MaxwellProducerMetrics producerMetrics;

	public AbstractProducer(MaxwellContext context) {
		this.context = context;
		this.outputConfig = context.getConfig().outputConfig;
		this.producerMetrics = context.getProducerMetrics();
	}

	abstract public void push(RowMap r) throws Exception;

	public StoppableTask getStoppableTask() {
		return null;
	}

	public MaxwellDiagnostic getDiagnostic() {
		return null;
	}

	public MaxwellProducerMetrics getProducerMetrics() {
		return producerMetrics;
	}
}
