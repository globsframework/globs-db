package org.globsframework.sql.constraints.impl;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.model.Key;
import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;

import java.util.HashMap;
import java.util.Map;

public class KeyConstraint {
    private GlobType globType;
    private Map<Field, Object> values = new HashMap<Field, Object>();

    public KeyConstraint(GlobType globType) {
        this.globType = globType;
    }

    public void setValue(Key key) {
        key.safeApplyOnKeyField((field, value) -> {
            this.values.put(field, value);
        });
        if (key.getGlobType() != globType) {
            throw new UnexpectedApplicationState("Bad key received was " + key.getGlobType().getName() +
                    " but " + globType.getName() + " was expected");
        }
    }

    public void setValue(Field field, Object value) {
        values.put(field, value);
    }

    public Object getValue(Field field) {
        return values.get(field);
    }

    public GlobType getGlobType() {
        return globType;
    }
}
