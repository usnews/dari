package com.psddev.dari.db;

import com.psddev.dari.util.Ping;

/** Pings the default database to see if it's available. */
public class DefaultDatabasePing implements Ping {

    @Override
    public void ping() {
        Query.from(Object.class).first();
    }
}
