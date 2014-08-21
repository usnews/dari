package com.psddev.dari.util.sa;

import java.lang.reflect.Method;
import java.util.List;

public abstract class JvmMethodListener {

    public abstract void onInvocation(
            Method callingMethod,
            int callingLine,
            Method calledMethod,
            int calledLine,
            JvmObject calledObject,
            List<JvmObject> calledArguments,
            JvmObject returnedObject);
}
