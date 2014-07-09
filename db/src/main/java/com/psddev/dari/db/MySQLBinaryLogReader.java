package com.psddev.dari.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventHeaderV4Deserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.FormatDescriptionEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.GtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.RotateEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.RowsQueryEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.TableMapEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.XidEventDataDeserializer;
import com.google.common.cache.Cache;
import com.psddev.dari.db.shyiko.DariDeleteRowsEventDataDeserializer;
import com.psddev.dari.db.shyiko.DariQueryEventDataDeserializer;
import com.psddev.dari.db.shyiko.DariUpdateRowsEventDataDeserializer;
import com.psddev.dari.db.shyiko.DariWriteRowsEventDataDeserializer;
import com.psddev.dari.util.ObjectUtils;

class MySQLBinaryLogReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBinaryLogReader.class);
    private static final Pattern MYSQL_JDBC_URL_PATTERN = Pattern.compile("(?i)jdbc:mysql://([^:/]+)(?::(\\d+))?/([^?]+).*");

    private final BinaryLogClient client;
    private final AtomicBoolean running = new AtomicBoolean();

    public MySQLBinaryLogReader(Cache<UUID, byte[][]> cache, DataSource dataSource) {
        String jdbcUrl = null;
        String username = null;
        String password = null;

        try {
            if (dataSource instanceof org.apache.tomcat.dbcp.dbcp.BasicDataSource) {
                org.apache.tomcat.dbcp.dbcp.BasicDataSource nativeDataSource = (org.apache.tomcat.dbcp.dbcp.BasicDataSource) dataSource;
                jdbcUrl = nativeDataSource.getUrl();
                username = nativeDataSource.getUsername();
                password = nativeDataSource.getPassword();

            } else if (dataSource instanceof org.apache.commons.dbcp2.BasicDataSource) {
                org.apache.commons.dbcp2.BasicDataSource nativeDataSource = (org.apache.commons.dbcp2.BasicDataSource) dataSource;
                jdbcUrl = nativeDataSource.getUrl();
                username = nativeDataSource.getUsername();
                password = nativeDataSource.getPassword();

            } else if (dataSource instanceof com.jolbox.bonecp.BoneCPDataSource) {
                com.jolbox.bonecp.BoneCPDataSource nativeDataSource = (com.jolbox.bonecp.BoneCPDataSource) dataSource;
                jdbcUrl = nativeDataSource.getJdbcUrl();
                username = nativeDataSource.getUsername();
                password = nativeDataSource.getPassword();

            } else {
                throw new IllegalArgumentException(String.format(
                        "Can't extract MySQL information from unknown data source [%s]!",
                        dataSource.getClass().getName()));
            }

        } catch (NoClassDefFoundError error) {
            throw new IllegalArgumentException(String.format(
                    "Can't extract MySQL information from data source [%s]!",
                    dataSource.getClass().getName()),
                    error);
        }

        Matcher jdbcUrlMatcher = MYSQL_JDBC_URL_PATTERN.matcher(jdbcUrl);

        if (!jdbcUrlMatcher.matches()) {
            throw new IllegalArgumentException(String.format(
                    "[%s] isn't a valid MySQL JDBC URL!",
                    jdbcUrl));
        }

        String catalog = jdbcUrlMatcher.group(3);
        this.client = new BinaryLogClient(
                jdbcUrlMatcher.group(1),
                ObjectUtils.firstNonNull(ObjectUtils.to(Integer.class, jdbcUrlMatcher.group(2)), 3306),
                catalog,
                username,
                password);

        client.registerLifecycleListener(new MySQLBinaryLogLifecycleListener(cache));
        client.registerEventListener(new MySQLBinaryLogEventListener(cache, catalog));

        @SuppressWarnings("rawtypes")
        Map<EventType, EventDataDeserializer> eventDataDeserializers = new HashMap<EventType, EventDataDeserializer>();
        Map<Long, TableMapEventData> tableMapEventByTableId = new HashMap<Long, TableMapEventData>();

        eventDataDeserializers.put(EventType.FORMAT_DESCRIPTION, new FormatDescriptionEventDataDeserializer());
        eventDataDeserializers.put(EventType.ROTATE, new RotateEventDataDeserializer());
        eventDataDeserializers.put(EventType.QUERY, new DariQueryEventDataDeserializer());
        eventDataDeserializers.put(EventType.TABLE_MAP, new TableMapEventDataDeserializer());
        eventDataDeserializers.put(EventType.XID, new XidEventDataDeserializer());
        eventDataDeserializers.put(EventType.WRITE_ROWS, new DariWriteRowsEventDataDeserializer(tableMapEventByTableId));
        eventDataDeserializers.put(EventType.UPDATE_ROWS, new DariUpdateRowsEventDataDeserializer(tableMapEventByTableId));
        eventDataDeserializers.put(EventType.DELETE_ROWS, new DariDeleteRowsEventDataDeserializer(tableMapEventByTableId));
        eventDataDeserializers.put(EventType.EXT_WRITE_ROWS, new DariWriteRowsEventDataDeserializer(tableMapEventByTableId).setMayContainExtraInformation(true));
        eventDataDeserializers.put(EventType.EXT_UPDATE_ROWS, new DariUpdateRowsEventDataDeserializer(tableMapEventByTableId).setMayContainExtraInformation(true));
        eventDataDeserializers.put(EventType.EXT_DELETE_ROWS, new DariDeleteRowsEventDataDeserializer(tableMapEventByTableId).setMayContainExtraInformation(true));
        eventDataDeserializers.put(EventType.ROWS_QUERY, new RowsQueryEventDataDeserializer());
        eventDataDeserializers.put(EventType.GTID, new GtidEventDataDeserializer());

        client.setEventDeserializer(
                new EventDeserializer(
                        new EventHeaderV4Deserializer(),
                        new NullEventDataDeserializer(),
                        eventDataDeserializers,
                        tableMapEventByTableId));
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            Thread connectThread = new Thread() {

                @Override
                public void run() {
                    try {
                        client.connect();

                    } catch (IOException error) {
                        LOGGER.warn("Can't connect to MySQL as a slave!", error);
                    }
                }
            };

            connectThread.start();
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            try {
                client.disconnect();

            } catch (IOException error) {
                LOGGER.warn("Can't disconnect from MySQL as a slave!", error);
            }
        }
    }
}
