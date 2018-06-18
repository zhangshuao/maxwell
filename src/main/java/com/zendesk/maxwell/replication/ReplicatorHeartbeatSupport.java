package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ReplicatorHeartbeatSupport {
	static final Logger LOGGER = LoggerFactory.getLogger(ReplicatorHeartbeatSupport.class);

	private final String clientID;
	private Long stopAtHeartbeat;
	private final HeartbeatNotifier notifier;
	private Position lastHeartbeatReadPosition;

	public ReplicatorHeartbeatSupport(String clientID, HeartbeatNotifier notifier, Position initialPosition) {
		this.clientID = clientID;
		this.notifier = notifier;
		this.lastHeartbeatReadPosition = initialPosition;
	}

	public Long getLastHeartbeatRead() {
		return lastHeartbeatReadPosition.getLastHeartbeatRead();
	}

	public void setStopAtHeartbeat(long heartbeat) {
		this.stopAtHeartbeat = heartbeat;
	}

	public boolean shouldStop(Position position) {
		if (stopAtHeartbeat != null) {
			long thisHeartbeat = position.getLastHeartbeatRead();
			return ( thisHeartbeat >= stopAtHeartbeat );
		} else
			return false;
	}

	/**
	 * If the input RowMap is one of the heartbeat pulses we sent out,
	 * process it.  If it's one of our heartbeats, we build a `HeartbeatRowMap`,
	 * which will be handled specially in producers (namely, it causes the binlog position to advance).
	 * It is isn't, we leave the row as a RowMap and the rest of the chain will ignore it.
	 *
	 * @return either a RowMap or a HeartbeatRowMap
	 */

	public RowMap processHeartbeats(RowMap row) {
		String hbClientID = (String) row.getData("client_id");
		if ( !Objects.equals(hbClientID, this.clientID) )
			return row; // plain row -- do not process.

		long rowHeartbeat = (Long) row.getData("heartbeat");
		LOGGER.debug("replicator picked up heartbeat: " + rowHeartbeat);
		this.lastHeartbeatReadPosition = row.getPosition().withHeartbeat(rowHeartbeat);
		notifier.heartbeat(rowHeartbeat);
		return HeartbeatRowMap.valueOf(row.getDatabase(), this.lastHeartbeatReadPosition, row.getNextPosition().withHeartbeat(rowHeartbeat));
	}

}
