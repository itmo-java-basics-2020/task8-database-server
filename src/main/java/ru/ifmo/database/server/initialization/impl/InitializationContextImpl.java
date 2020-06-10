package ru.ifmo.database.server.initialization.impl;

import lombok.Builder;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.initialization.TableInitializationContext;

@Builder
public class InitializationContextImpl implements InitializationContext {

    private InitializationContextImpl(ExecutionEnvironment executionEnvironment,
                                      DatabaseInitializationContext currentDatabaseContext,
                                      TableInitializationContext currentTableContext,
                                      SegmentInitializationContext currentSegmentContext) {
        throw new UnsupportedOperationException(); // todo implement
    }

    public static InitializationContext builder() {
    }

    @Override
    public ExecutionEnvironment executionEnvironment() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public DatabaseInitializationContext currentDbContext() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public TableInitializationContext currentTableContext() {
        throw new UnsupportedOperationException(); // todo implement
    }

    @Override
    public SegmentInitializationContext currentSegmentContext() {
        throw new UnsupportedOperationException(); // todo implement
    }
}