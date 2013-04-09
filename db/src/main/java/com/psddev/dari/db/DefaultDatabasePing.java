package com.psddev.dari.db;

import com.psddev.dari.util.Ping;

/** Pings the default database to see if it's available. */
public class DefaultDatabasePing implements Ping {

    @Override
    @SuppressWarnings("unchecked")
    public void ping() {
        Database defaultDatabase = Database.Static.getDefault();

        if (defaultDatabase instanceof Iterable) {
            for (Database delegate : (Iterable<Database>) defaultDatabase) {
                Query.from(Object.class).using(delegate).first();
            }

        } else {
            Query.from(Object.class).first();
        }
    }
}
