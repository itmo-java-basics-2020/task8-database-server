package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.*;
import ru.ifmo.database.server.logic.Table;
import ru.ifmo.database.server.logic.impl.CachingTable;
import ru.ifmo.database.server.logic.impl.TableImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableInitializer implements Initializer {

    private final Initializer segmentInitializer;

    public TableInitializer(Initializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        ExecutionEnvironment env = context.executionEnvironment();
        DatabaseInitializationContext dbContext = context.currentDbContext();
        TableInitializationContext tableContext = context.currentTableContext();
        Path tablePath = tableContext.getTablePath();

        List<Path> segmentPaths;
        try (Stream<Path> walk = Files.walk(tablePath)) {
            segmentPaths = walk.filter(Files::isRegularFile)
                    .filter(p -> tablePath.equals(p.getParent()))
                    .collect(Collectors.toList());
        } catch (IOException ex) {
            throw new DatabaseException(ex);
        }
        Collections.sort(segmentPaths);

        for (Path segmentPath : segmentPaths) {
            int segmentSize;
            try {
                segmentSize = (int) Files.size(segmentPath);
            } catch (IOException ex) {
                throw new DatabaseException(ex);
            }
            SegmentInitializationContext segmentContext = new SegmentInitializationContextImpl(
                    segmentPath.getFileName().toString(),
                    segmentPath,
                    segmentSize,
                    new SegmentIndex()
            );
            InitializationContext initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(env)
                    .currentDatabaseContext(dbContext)
                    .currentTableContext(tableContext)
                    .currentSegmentContext(segmentContext)
                    .build();
            segmentInitializer.perform(initializationContext);
        }

        Table table = TableImpl.initializeFromContext(tableContext);
        dbContext.addTable(new CachingTable(table));
    }
}
