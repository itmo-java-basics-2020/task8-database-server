package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

import java.io.File;

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
        File dir = new File(context.currentDbContext().getDatabasePath().toString());
        if (dir.listFiles() == null) {
            throw new DatabaseException("Not correct DB directory path");
        }
        //noinspection ConstantConditions
        for (File table : dir.listFiles(File::isDirectory)) {
            InitializationContext initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(new TableInitializationContextImpl(table.getName(), table.toPath(), new TableIndex()))
                    .currentSegmentContext(context.currentSegmentContext()).build();
            tableInitializer.perform(initializationContext);
        }
        context.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(context.currentDbContext()));
    }
}