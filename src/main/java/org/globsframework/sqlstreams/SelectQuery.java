package org.globsframework.sqlstreams;

import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.streams.DbStream;
import org.globsframework.utils.exceptions.ItemNotFound;
import org.globsframework.utils.exceptions.TooManyItems;

import java.util.stream.Stream;

public interface SelectQuery extends AutoCloseable {
    Stream<?> executeAsStream();

    Stream<Glob> executeAsGlobStream();

    DbStream execute();

    GlobList executeAsGlobs();

    Glob executeUnique() throws ItemNotFound, TooManyItems;

//    <T> CompletableFuture<T> executeAsFutureStream(Consumer<?> consumer, Consumer<?> onComplete);
//
//    <T> CompletableFuture<T> executeAsFutureGlobStream(Consumer<?> consumer, Consumer<?> onComplete);
//
//    <T> CompletableFuture<T> executeAsFutureGlobs(Consumer<?> consumer, Consumer<?> onComplete);

    void close();
}
