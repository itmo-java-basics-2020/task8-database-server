package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.impl.SegmentImpl;
import ru.ifmo.database.server.logic.impl.TableImpl;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

public class TableInitializer implements Initializer {

    private final Initializer segmentInitializer;

    public TableInitializer(Initializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        File file = new File(context.currentTableContext().getTablePath().toString());
        String[] segments = file.list((current, name) -> new File(current, name).isFile());
        if (segments == null) {
            return;
        }
        Arrays.sort(segments);
        for (String segment : segments) {
            context.currentTableContext().updateCurrentSegment(SegmentImpl.create(segment,
                    context.currentTableContext().getTablePath()));
            InitializationContextImpl initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(context.currentTableContext())
                    .currentSegmentContext(new SegmentInitializationContextImpl(segment,
                            Path.of(context.currentTableContext().getTablePath().toString()),
                            (int) new File(context.currentTableContext().getTablePath().toString() + "\\" + segment).length(),
                            new SegmentIndex()))
                    .build();
            segmentInitializer.perform(initializationContext);
        }
        context.currentDbContext()
                .addTable(TableImpl.initializeFromContext(context.currentTableContext()));
    }
}
