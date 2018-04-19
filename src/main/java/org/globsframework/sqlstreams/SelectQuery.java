package org.globsframework.sqlstreams;

import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.streams.GlobStream;
import org.globsframework.utils.exceptions.ItemNotFound;
import org.globsframework.utils.exceptions.TooManyItems;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface SelectQuery extends AutoCloseable {
    Stream<?> executeAsStream();

    GlobStream execute();

    GlobList executeAsGlobs();

    Glob executeUnique() throws ItemNotFound, TooManyItems;

//    <T> CompletableFuture<T> executeAsFutureStream(Consumer<?> consumer, Consumer<?> onComplete);
//
//    <T> CompletableFuture<T> executeAsFutureGlobStream(Consumer<?> consumer, Consumer<?> onComplete);
//
//    <T> CompletableFuture<T> executeAsFutureGlobs(Consumer<?> consumer, Consumer<?> onComplete);

    void close();
}
