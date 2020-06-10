package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.impl.TableImpl;

import java.io.File;

public class TableInitializer implements Initializer {

    private final Initializer segmentInitializer;

    public TableInitializer(Initializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (context.currentTableContext() == null) {
            throw new DatabaseException("Context Table is null");
        }
        //getting table dir
        File dir = context.currentTableContext().getTablePath().toFile();

        if (dir.listFiles() == null) {
            return;
        }
        //Search for all segments
        File[] segments = dir.listFiles();

        for (File seg : segments) {
            InitializationContext init = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .databaseInitializationContext(context.currentDbContext())
                    .tableInitializationContext(context.currentTableContext())
                    .segmentInitializationContext(new SegmentInitializationContextImpl(
                                    seg.getName(), seg.toPath(), (int)seg.length(),
                                    new SegmentIndex(), true))
                    .build();

            segmentInitializer.perform(init);
        }

        context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
    }
}
