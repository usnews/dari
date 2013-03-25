package com.psddev.dari.db;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;

/**
 * Skeletal database implementation that forwards all calls
 * to another database.
 */
public abstract class ForwardingDatabase implements Database {

    public static final String DELEGATE_SETTING = "delegate";

    private Database delegate;

    /** Returns the database where all calls are forwarded to. */
    public Database getDelegate() {
        return delegate;
    }

    /** Sets the database where all calls are forwarded to. */
    public void setDelegate(Database delegate) {
        this.delegate = delegate;
    }

    // --- Database support ---

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        setDelegate(Database.Static.getInstance(ObjectUtils.to(String.class, settings.get(DELEGATE_SETTING))));
    }

    @Override
    public String getName() {
        return getDelegate().getName();
    }

    @Override
    public void setName(String name) {
        getDelegate().setName(name);
    }

    @Override
    public DatabaseEnvironment getEnvironment() {
        return getDelegate().getEnvironment();
    }

    @Override
    public void setEnvironment(DatabaseEnvironment environment) {
        getDelegate().setEnvironment(environment);
    }

    @Override
    public <T> List<T> readAll(Query<T> query) {
        return getDelegate().readAll(query);
    }

    @Deprecated
    @Override
    public <T> List<T> readList(Query<T> query) {
        return getDelegate().readList(query);
    }

    @Override
    public <T> List<Grouping<T>> readAllGrouped(Query<T> query, String... fields) {
        return getDelegate().readAllGrouped(query, fields);
    }

    @Override
    public long readCount(Query<?> query) {
        return getDelegate().readCount(query);
    };

    @Override
    public <T> T readFirst(Query<T> query) {
        return getDelegate().readFirst(query);
    }

    @Override
    public <T> Iterable<T> readIterable(Query<T> query, int fetchSize) {
        return getDelegate().readIterable(query, fetchSize);
    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        return getDelegate().readLastUpdate(query);
    }

    @Override
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {
        return getDelegate().readPartial(query, offset, limit);
    }

    @Override
    public <T> PaginatedResult<Grouping<T>> readPartialGrouped(Query<T> query, long offset, int limit, String... fields) {
        return getDelegate().readPartialGrouped(query, offset, limit, fields);
    }

    @Deprecated
    @Override
    public Map<Object, Long> readGroupedCount(Query<?> query, String field) {
        return getDelegate().readGroupedCount(query, field);
    }

    @Override
    public boolean beginWrites() {
        return getDelegate().beginWrites();
    }

    @Override
    public void beginIsolatedWrites() {
        getDelegate().beginIsolatedWrites();
    }

    @Override
    public boolean commitWrites() {
        return getDelegate().commitWrites();
    }

    @Override
    public boolean commitWritesEventually() {
        return getDelegate().commitWritesEventually();
    }

    @Override
    public boolean endWrites() {
        return getDelegate().endWrites();
    }

    @Override
    public void save(State state) {
        getDelegate().save(state);
    }

    @Override
    public void saveUnsafely(State state) {
        getDelegate().saveUnsafely(state);
    }

    @Override
    public void index(State state) {
        getDelegate().index(state);
    }

    @Override
    public void indexAll(ObjectIndex index) {
        getDelegate().indexAll(index);
    }

    @Override
    public void delete(State state) {
        getDelegate().delete(state);
    }

    @Override
    public void deleteByQuery(Query<?> query) {
        getDelegate().deleteByQuery(query);
    }

    // --- Object support ---

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(super.toString());
        s.append("(delegate=");
        s.append(getDelegate().toString());
        s.append(")");
        return s.toString();
    }
}
