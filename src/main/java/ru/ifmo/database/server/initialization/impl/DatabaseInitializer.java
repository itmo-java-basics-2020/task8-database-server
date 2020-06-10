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
            throw new DatabaseException("DbContext must not be null");
        }

        File databaseDir = context.currentDbContext().getDatabasePath().toFile();
        if (databaseDir.listFiles() == null) {
            return;
        }

        for (File tableDir : databaseDir.listFiles()) {
            if (!tableDir.isDirectory()) {
                continue;
            }

            tableInitializer.perform(InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(new TableInitializationContextImpl(
                            tableDir.getName(), tableDir.toPath(), new TableIndex()
                    ))
                    .build());
        }

        context.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(context.currentDbContext()));
    }
}