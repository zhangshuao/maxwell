package com.zendesk.maxwell.monitoring;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.zendesk.maxwell.MaxwellContext;

public class MaxwellProducerMetrics {

	private final Counter succeededMessageCount;
	private final Meter succeededMessageMeter;
	private final Counter failedMessageCount;
	private final Meter failedMessageMeter;
	private final Timer metricsTimer;

	public MaxwellProducerMetrics(MaxwellContext context) {
		Metrics metrics = context.getMetrics();
		MetricRegistry metricRegistry = metrics.getRegistry();

		this.succeededMessageCount = metricRegistry.counter(metrics.metricName("messages", "succeeded"));
		this.succeededMessageMeter = metricRegistry.meter(metrics.metricName("messages", "succeeded", "meter"));
		this.failedMessageCount = metricRegistry.counter(metrics.metricName("messages", "failed"));
		this.failedMessageMeter = metricRegistry.meter(metrics.metricName("messages", "failed", "meter"));
		this.metricsTimer = metrics.getRegistry().timer(metrics.metricName("message", "publish", "time"));
	}

	public Counter getSucceededMessageCount() {
		return succeededMessageCount;
	}

	public Meter getSucceededMessageMeter() {
		return succeededMessageMeter;
	}

	public Counter getFailedMessageCount() {
		return failedMessageCount;
	}

	public Meter getFailedMessageMeter() {
		return failedMessageMeter;
	}

	public Timer getMetricsTimer() {
		return metricsTimer;
	}
}
