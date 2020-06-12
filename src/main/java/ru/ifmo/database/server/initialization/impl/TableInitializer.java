package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.Table;
import ru.ifmo.database.server.logic.impl.CachingTable;
import ru.ifmo.database.server.logic.impl.TableImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.stream.Stream;

public class TableInitializer implements Initializer {

    private final Initializer segmentInitializer;

    public TableInitializer(Initializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (context.currentTableContext() == null) {
            throw new DatabaseException("Segment context equals zero");
        }

        Path dir = context.currentTableContext().getTablePath();
        Stream<Path> paths;
        try {
            paths = Files.list(dir);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        ArrayList<InitializationContext> contexts = context.currentTableContext().getInitializationContexts();
        paths.forEach(segment -> {
            InitializationContext initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(context.currentTableContext())
                    .currentSegmentContext(new SegmentInitializationContextImpl(segment.getFileName().toString(),
                            segment, new SegmentIndex(), segment.toFile().length()))
                    .build();
            try {
                contexts.add(initializationContext);
                segmentInitializer.perform(initializationContext);
            } catch (DatabaseException e) {
                e.printStackTrace();
            }
        });
        Table cachingTable = new CachingTable(TableImpl.initializeFromContext(context.currentTableContext()));
        context.currentDbContext().addTable(cachingTable);

    }
}
