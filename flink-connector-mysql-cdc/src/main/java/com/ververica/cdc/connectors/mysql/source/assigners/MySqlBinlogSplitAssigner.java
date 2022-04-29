/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.assigners;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
import com.ververica.cdc.connectors.mysql.source.assigners.state.BinlogPendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.createMySqlConnection;
import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.currentBinlogOffset;

/**
 * A {@link MySqlSplitAssigner} which only read binlog from current binlog position.
 */
public class MySqlBinlogSplitAssigner implements MySqlSplitAssigner {

	private static final Logger LOG = LoggerFactory.getLogger(MySqlBinlogSplitAssigner.class);
	private static final String BINLOG_SPLIT_ID = "binlog-split";

	private final MySqlSourceConfig sourceConfig;

	private boolean isBinlogSplitAssigned;
	private BinlogOffset binlogOffset;

	public MySqlBinlogSplitAssigner(MySqlSourceConfig sourceConfig) {
		this(sourceConfig, false);
	}

	private MySqlBinlogSplitAssigner(
			MySqlSourceConfig sourceConfig, boolean isBinlogSplitAssigned) {
		this.sourceConfig = sourceConfig;
		this.isBinlogSplitAssigned = isBinlogSplitAssigned;
	}

	public MySqlBinlogSplitAssigner(
			MySqlSourceConfig sourceConfig, BinlogPendingSplitsState checkpoint) {
		this(sourceConfig, checkpoint.isBinlogSplitAssigned());
	}

	@Override
	public void open() {
		if (fromSpecificOffset()) {
			if (binlogAvailable()) {
				this.binlogOffset = new BinlogOffset(
						sourceConfig.getStartupOptions().specificOffsetFile,
						sourceConfig.getStartupOptions().specificOffsetPos
				);
			}
		}
	}

	private boolean binlogAvailable() {
		String binlogFilename = sourceConfig.getStartupOptions().specificOffsetFile;
		// binlog name must be set to a non-empty string
		if (binlogFilename == null || binlogFilename.equals("")) {
			return false;
		}

		// Accumulate the available binlog filenames ...
		MySqlConnection connection = createMySqlConnection(sourceConfig.getDbzConfiguration());
		List<String> logNames = connection.availableBinlogFiles();

		// And compare with the one we're supposed to use ...
		boolean found = logNames.stream().anyMatch(binlogFilename::equals);
		if (!found) {
			LOG.info(
					"Connector requires binlog file '{}', but MySQL only has {}",
					binlogFilename,
					String.join(", ", logNames));
		} else {
			LOG.info("MySQL has the binlog file '{}' required by the connector", binlogFilename);
		}
		return found;
	}

	private boolean fromSpecificOffset() {
		return Objects.equals(sourceConfig.getStartupOptions().startupMode, StartupMode.SPECIFIC_OFFSETS);
	}

	@Override
	public Optional<MySqlSplit> getNext() {
		if (isBinlogSplitAssigned) {
			return Optional.empty();
		} else {
			isBinlogSplitAssigned = true;
			return Optional.of(createBinlogSplit());
		}
	}

	@Override
	public boolean waitingForFinishedSplits() {
		return false;
	}

	@Override
	public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
		return Collections.EMPTY_LIST;
	}

	@Override
	public void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets) {
		// do nothing
	}

	@Override
	public void addSplits(Collection<MySqlSplit> splits) {
		// we don't store the split, but will re-create binlog split later
		isBinlogSplitAssigned = false;
	}

	@Override
	public PendingSplitsState snapshotState(long checkpointId) {
		return new BinlogPendingSplitsState(isBinlogSplitAssigned);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		// nothing to do
	}

	@Override
	public AssignerStatus getAssignerStatus() {
		return AssignerStatus.INITIAL_ASSIGNING_FINISHED;
	}

	@Override
	public void suspend() {
	}

	@Override
	public void wakeup() {
	}

	@Override
	public void close() {
	}

	// ------------------------------------------------------------------------------------------

	private MySqlBinlogSplit createBinlogSplit() {
		if (fromSpecificOffset() && binlogOffset != null) {
			new MySqlBinlogSplit(
					BINLOG_SPLIT_ID,
					binlogOffset,
					BinlogOffset.NO_STOPPING_OFFSET,
					new ArrayList<>(),
					new HashMap<>(),
					0);
		}
		try (JdbcConnection jdbc = DebeziumUtils.openJdbcConnection(sourceConfig)) {
			return new MySqlBinlogSplit(
					BINLOG_SPLIT_ID,
					currentBinlogOffset(jdbc),
					BinlogOffset.NO_STOPPING_OFFSET,
					new ArrayList<>(),
					new HashMap<>(),
					0);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Read the binlog offset error", e);
		}
	}
}
