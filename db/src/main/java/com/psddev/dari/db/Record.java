package com.psddev.dari.db;

import com.psddev.dari.util.HtmlObject;
import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.TypeDefinition;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/** Represents a generic record. */
public class Record implements Cloneable, Comparable<Record>, HtmlObject, Recordable {

    /**
     * Creates a blank instance. This method is protected so that the class
     * can behave like an abstract class, and still allow instantiation
     * through reflection when necessary.
     */
    protected Record() {
    }

    /**
     * Triggers right before this record is saved to the given
     * {@code database}. Default implementation of this method
     * doesn't do anything.
     */
    protected void beforeSave() {
        beforeSave(getState().getDatabase());
    }

    /**
     * Triggers right before this record is deleted in the given
     * {@code database}. Default implementation of this method
     * doesn't do anything.
     */
    protected void beforeDelete() {
        beforeDelete(getState().getDatabase());
    }

    // --- Cloneable support ---

    @Override
    public Object clone() {
        Object clone = TypeDefinition.getInstance(getClass()).newInstance();
        State cloneState = State.getInstance(clone);
        cloneState.setDatabase(state.getDatabase());
        cloneState.setStatus(state.getStatus());
        cloneState.setValues(state.getSimpleValues());
        return clone;
    }

    // --- Comparable support ---

    @Override
    public int compareTo(Record other) {
        return ObjectUtils.compare(getLabel(), other.getLabel(), true);
    }

    // --- HtmlObject support ---

    @Override
    public void format(HtmlWriter writer) throws IOException {
        writer.start("pre");

            State state = getState();
            ObjectType type = state.getType();
            if (type != null) {
                writer.html(type.getInternalName());
                writer.html(": ");
            }

            writer.start("a",
                    "target", "_blank",
                    "href", StringUtils.addQueryParameters(
                            "/_debug/query",
                            "where", "id = " + state.getId(),
                            "action", "Run"));
                writer.html(getLabel());
            writer.end();
            writer.tag("br");

            writer.html(ObjectUtils.toJson(state.getSimpleValues(), true));

        writer.end();
    }

    // --- Recordable support ---

    private transient State state;

    @Override
    public State getState() {
        if (state == null) {
            setState(new State());
        }
        return state;
    }

    @Override
    public void setState(State state) {
        if (this.state != null) {
            this.state.unlinkObject(this);
        }
        this.state = state;
        this.state.linkObject(this);
    }

    @Override
    public <T> T as(Class<T> modificationClass) {
        return getState().as(modificationClass);
    }

    // --- Object support ---

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object != null) {
            try {
                return getState().equals(State.getInstance(object));
            } catch (RuntimeException ex) {
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getState().hashCode();
    }

    @Override
    public String toString() {
        return getState().toString();
    }

    // --- PredicateParser bridge ---

    /** @see PredicateParser.Static#evaluate(Object, Predicate) */
    public boolean is(Predicate predicate) {
        return PredicateParser.Static.evaluate(this, predicate);
    }

    /** @see PredicateParser.Static#evaluate(Object, String, Object...) */
    public boolean is(String predicateString, Object... parameters) {
        return PredicateParser.Static.evaluate(this, predicateString, parameters);
    }

    // --- Query bridge ---

    /** @see Query#from */
    public <T> Query<T> queryFrom(Class<T> objectClass) {
        Query<T> query = Query.from(objectClass);
        query.setDatabase(getState().getDatabase());
        return query;
    }

    // --- State bridge ---

    /** @see State#beginWrites() */
    public boolean beginWrites() {
        return getState().beginWrites();
    }

    /** @see State#commitWrites() */
    public boolean commitWrites() {
        return getState().commitWrites();
    }

    /** @see State#delete() */
    public void delete() {
        getState().delete();
    }

    /** @see State#endWrites() */
    public boolean endWrites() {
        return getState().endWrites();
    }

    /** @see State#getId() */
    public UUID getId() {
        return getState().getId();
    }

    /** @see State#getExtra(String) */
    public Object getExtra(String name) {
        return getState().getExtra(name);
    }

    /**
     * Returns a descriptive label for this record.
     *
     * @see State#getDefaultLabel
     */
    public String getLabel() {
        return getState().getDefaultLabel();
    }

    /** @see State#save() */
    public void save() {
        getState().save();
    }

    /** @see State#saveImmediately */
    public void saveImmediately() {
        getState().saveImmediately();
    }

    /** @see State#saveEventually */
    public void saveEventually() {
        getState().saveEventually();
    }

    // --- JSTL support ---

    public Map<String, Object> getAs() {
        return getState().getAs();
    }

    // --- Deprecated ---

    /** @deprecated Use {@link #beforeSave()} instead. */
    @Deprecated
    protected void beforeSave(Database database) {
    }

    /** @deprecated No replacement. */
    @Deprecated
    protected void afterSave(Database database) {
    }

    /** @deprecated Use {@link #beforeDelete()} instead. */
    @Deprecated
    protected void beforeDelete(Database database) {
    }

    /** @deprecated No replacement. */
    @Deprecated
    protected void afterDelete(Database database) {
    }

    /** @deprecated Use {@link #getAs} instead. */
    @Deprecated
    public Map<String, Object> getModifications() {
        return getAs();
    }
}
