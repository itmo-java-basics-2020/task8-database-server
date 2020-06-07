package ru.ifmo.database.server.initialization.impl.initializer;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.impl.context.InitializationContextImpl;
import ru.ifmo.database.server.initialization.impl.context.SegmentInitializationContextImpl;
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
            throw new DatabaseException("Table skipped context during initialization");
        }

        File tableDir = context.currentTableContext().getTablePath().toFile();
        // Stop initialization if table directory is empty
        if (tableDir.listFiles() == null) {
            return;
        }

        // We should run in alphabetical order
        File[] segments = tableDir.listFiles();
        Arrays.sort(segments);

        for (int i = 0; i < segments.length; i++) {
            // Skip directories
            if (segments[i].isDirectory()) {
                continue;
            }

            // Skip big files
            if (segments[i].length() > Integer.MAX_VALUE) {
                continue;
            }

            segmentInitializer.perform(InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(context.currentTableContext())
                    .currentSegmentContext(new SegmentInitializationContextImpl(
                            segments[i].getName(), segments[i].toPath(), segments[i].length(),
                            i != segments.length - 1, new SegmentIndex()
                    ))
                    .build());
        }

        // Add a table to the database after the segment -> table
        context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
    }
}
