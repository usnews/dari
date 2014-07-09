package com.psddev.dari.db;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.AbstractLifecycleListener;
import com.google.common.cache.Cache;

class MySQLBinaryLogLifecycleListener extends AbstractLifecycleListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBinaryLogLifecycleListener.class);

    private final Cache<UUID, byte[][]> cache;

    public MySQLBinaryLogLifecycleListener(Cache<UUID, byte[][]> cache) {
        this.cache = cache;
    }

    @Override
    public void onCommunicationFailure(BinaryLogClient client, Exception error) {
        LOGGER.warn("Can't communicate with MySQL as a slave!", error);
    }

    @Override
    public void onDisconnect(BinaryLogClient client) {
        LOGGER.warn("Disconnected from MySQL as a slave so invalidating cache");
        cache.invalidateAll();
    }
}
