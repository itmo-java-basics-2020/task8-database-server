package ru.ifmo.database.server.initialization.impl.initializer;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.impl.context.InitializationContextImpl;
import ru.ifmo.database.server.initialization.impl.context.TableInitializationContextImpl;
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
            throw new DatabaseException("Database skipped context during initialization");
        }

        File databaseDir = context.currentDbContext().getDatabasePath().toFile();
        // Stop initialization if database directory is empty
        if (databaseDir.listFiles() == null) {
            return;
        }

        for (File tableDir : databaseDir.listFiles()) {
            // Skip files
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

        // Add a database to the database server after the segment -> table -> database
        context.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(context.currentDbContext()));
    }
}