package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
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
        DatabaseInitializationContext dbContext = context.currentDbContext();
        if (dbContext == null) {
            throw new DatabaseException("Database context is null");
        }

        File dbDirectory = dbContext.getDatabasePath().toFile();

        if (dbDirectory.listFiles() == null) {
            return;
        }

        for (File tableDirectory : dbDirectory.listFiles()) {
            if (tableDirectory.isDirectory()) {
                this.tableInitializer.perform(InitializationContextImpl.builder()
                        .executionEnvironment(context.executionEnvironment())
                        .currentDatabaseContext(dbContext)
                        .currentTableContext(
                                new TableInitializationContextImpl(
                                        tableDirectory.getName(),
                                        dbContext.getDatabasePath(),
                                        new TableIndex()))
                        .build());
            }
        }
        context.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(dbContext));
    }
}