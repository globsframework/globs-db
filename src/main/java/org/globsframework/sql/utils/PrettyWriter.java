package org.globsframework.sql.utils;

public interface PrettyWriter {
    StringPrettyWriter append(String s);

    void appendIf(String s, boolean shouldAppend);

    PrettyWriter newLine();

    PrettyWriter removeLast();
}
