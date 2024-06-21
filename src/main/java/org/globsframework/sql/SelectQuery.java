package org.globsframework.sql;

import org.globsframework.model.FieldValues;
import org.globsframework.model.Glob;
import org.globsframework.streams.DbStream;
import org.globsframework.utils.exceptions.ItemNotFound;
import org.globsframework.utils.exceptions.TooManyItems;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public interface SelectQuery extends AutoCloseable {
    Stream<?> executeAsStream();

    Stream<Glob> executeAsGlobStream();

    Stream<FieldValues> executeAsFieldValuesStream();

    DbStream execute();

    List<Glob> executeAsGlobs();

//    Collection<Glob> executeAsGlob();

    default Glob executeUnique() throws ItemNotFound, TooManyItems {
        return executeOne().orElseThrow(() -> new ItemNotFound("For " + toString()));
    }

    default Optional<Glob> executeOne() throws TooManyItems {
        List<Glob> globs = executeAsGlobs();
        if (globs.size() == 1) {
            return Optional.of(globs.get(0));
        }
        if (globs.isEmpty()) {
            return Optional.empty();
        }
        throw new TooManyItems("Too many results for: " + toString());
    }

//    <T> CompletableFuture<T> executeAsFutureStream(Consumer<?> consumer, Consumer<?> onComplete);
//
//    <T> CompletableFuture<T> executeAsFutureGlobStream(Consumer<?> consumer, Consumer<?> onComplete);
//
//    <T> CompletableFuture<T> executeAsFutureGlobs(Consumer<?> consumer, Consumer<?> onComplete);

    void close();
}
