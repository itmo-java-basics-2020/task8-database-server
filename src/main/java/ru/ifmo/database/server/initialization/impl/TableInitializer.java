package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.impl.TableImpl;

import java.io.File;

public class TableInitializer implements Initializer {

    private final Initializer segmentInitializer;

    public TableInitializer(Initializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        TableInitializationContext tableContext = context.currentTableContext();
        if (tableContext == null) {
            throw new DatabaseException("Table context is null");
        }

        File tableDirectory = tableContext.getTablePath().toFile();

        if (tableDirectory.listFiles() == null) {
            return;
        }

        for (File segment : tableDirectory.listFiles()) {
            if (segment.isFile()) {
                this.segmentInitializer.perform(InitializationContextImpl.builder()
                        .executionEnvironment(context.executionEnvironment())
                        .currentDatabaseContext(context.currentDbContext())
                        .currentTableContext(tableContext)
                        .currentSegmentContext(
                                new SegmentInitializationContextImpl(
                                        segment.getName(),
                                        segment.toPath(),
                                        (int) segment.length(),
                                        new SegmentIndex(),
                                        segment.canRead()))
                        .build());
            }
        }
        context.currentDbContext()
                .addTable(TableImpl.initializeFromContext(tableContext));
    }
}
