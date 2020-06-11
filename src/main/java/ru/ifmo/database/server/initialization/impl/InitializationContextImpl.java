package ru.ifmo.database.server.initialization.impl;

import lombok.Builder;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.initialization.TableInitializationContext;

@Builder
public class InitializationContextImpl implements InitializationContext {
    private ExecutionEnvironment executionEnvironment;
    private DatabaseInitializationContext currentDatabaseContext;
    private TableInitializationContext currentTableContext;
    private SegmentInitializationContext currentSegmentContext;

    private InitializationContextImpl(ExecutionEnvironment executionEnvironment,
                                      DatabaseInitializationContext currentDatabaseContext,
                                      TableInitializationContext currentTableContext,
                                      SegmentInitializationContext currentSegmentContext) {
        this.executionEnvironment = executionEnvironment;
        this.currentDatabaseContext = currentDatabaseContext;
        this.currentTableContext = currentTableContext;
        this.currentSegmentContext = currentSegmentContext;
    }

    @Override
    public ExecutionEnvironment executionEnvironment() {
        return executionEnvironment;
    }

    @Override
    public DatabaseInitializationContext currentDbContext() {
        return currentDatabaseContext;
    }

    @Override
    public TableInitializationContext currentTableContext() {
        return currentTableContext;
    }

    @Override
    public SegmentInitializationContext currentSegmentContext() {
        return currentSegmentContext;
    }
}