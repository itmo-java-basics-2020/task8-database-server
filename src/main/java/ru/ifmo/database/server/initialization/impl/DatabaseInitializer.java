package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class DatabaseInitializer implements Initializer {
    private final Initializer tableInitializer;

    public DatabaseInitializer(Initializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        if (context.currentDbContext() == null) {
            throw new DatabaseException("DbContext equals null");
        }
        Path dir = context.currentDbContext().getDatabasePath();
        Stream<Path> paths;
        try {
            paths = Files.list(dir);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        paths.forEach(path -> {
            InitializationContext initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(new TableInitializationContextImpl(path.getFileName().toString(), path.toAbsolutePath(), new TableIndex()))
                    .currentSegmentContext(context.currentSegmentContext())
                    .build();
            try {
                tableInitializer.perform(initializationContext);
            } catch (DatabaseException e) {
                e.printStackTrace();
            }
        });
        context.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(context.currentDbContext()));
    }
}