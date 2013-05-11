package com.psddev.dari.db;

import java.io.IOException;
import java.io.Writer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.FormFilter;
import com.psddev.dari.util.FormProcessor;

public abstract class AbstractForm extends FormWriter implements FormProcessor {

    public AbstractForm() {
        this(null);
    }

    public AbstractForm(Writer writer) {
        super(writer);
    }

    @Override
    public final Object process(HttpServletRequest request, HttpServletResponse response) throws IOException {

        State state = getState(request);

        if (FormFilter.Static.isFormSubmitted(this, request)) {
            try {
                InputTag.Static.updateObject(this, state, request);

                processState(state, request, response);
                return state;

            } catch (IOException e) {
                throw e;

            } catch (Throwable e) {
                e.printStackTrace(); // TODO: Remove debug statement
            }
        }

        Writer originalDelegate = getDelegate();
        try {
            setDelegate(response.getWriter());
            writeState(state);

        } finally {
            setDelegate(originalDelegate);
        }

        return state;
    }

    protected abstract State getState(HttpServletRequest request);

    protected abstract void processState(State state, HttpServletRequest request, HttpServletResponse response) throws IOException;

    protected abstract void writeState(State state) throws IOException;
}
