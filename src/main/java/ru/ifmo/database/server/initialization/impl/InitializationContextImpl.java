package ru.ifmo.database.server.initialization.impl;

import lombok.Builder;
import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.initialization.TableInitializationContext;

@Builder
public class InitializationContextImpl implements InitializationContext {

    private final ExecutionEnvironment executionEnvironment;
    private final DatabaseInitializationContext currentDatabaseContext;
    private final TableInitializationContext currentTableContext;
    private final SegmentInitializationContext currentSegmentContext;

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
        //todo
        return null;
    }

    @Override
    public DatabaseInitializationContext currentDbContext() {
        //todo
        return null;
    }

    @Override
    public TableInitializationContext currentTableContext() {
        //todo
        return null;
    }

    @Override
    public SegmentInitializationContext currentSegmentContext() {
        //todo
        return null;
    }
}