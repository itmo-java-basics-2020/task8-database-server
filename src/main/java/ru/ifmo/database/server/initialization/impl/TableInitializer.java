package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.impl.TableImpl;

import java.io.File;
import java.util.Arrays;

public class TableInitializer implements Initializer {

    private final Initializer segmentInitializer;

    public TableInitializer(Initializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (context.currentTableContext() == null) {
            throw new DatabaseException("TableContext must not be null");
        }

        File tableDir = context.currentTableContext().getTablePath().toFile();
        if (tableDir.listFiles() == null) {
            return;
        }

        File[] segments = tableDir.listFiles();
        Arrays.sort(segments);

        for (int i = 0; i < segments.length; i++) {
            if (segments[i].isDirectory()) {
                continue;
            }

            if (segments[i].length() > Integer.MAX_VALUE) {
                continue;
            }

            segmentInitializer.perform(InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(context.currentTableContext())
                    .currentSegmentContext(new SegmentInitializationContextImpl(segments[i].getName(), segments[i].toPath(), (int) segments[i].length(), new SegmentIndex(), i != segments.length - 1))
                    .build());
        }

        context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
    }
}
