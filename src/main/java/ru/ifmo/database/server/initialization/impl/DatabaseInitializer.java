package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.DatabaseInitializationContext;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DatabaseInitializer implements Initializer {
    private final Initializer tableInitializer;

    public DatabaseInitializer(Initializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        ExecutionEnvironment env = context.executionEnvironment();
        DatabaseInitializationContext dbContext = context.currentDbContext();
        Path dbRoot = dbContext.getDatabasePath();

        List<Path> tablePaths;
        try (Stream<Path> walk = Files.walk(dbRoot)) {
            tablePaths = walk.filter(Files::isDirectory)
                    .filter(p -> dbRoot.equals(p.getParent()))
                    .collect(Collectors.toList());
        } catch (IOException ex) {
            throw new DatabaseException(ex);
        }

        for (Path tablePath : tablePaths) {
            TableInitializationContext tableContext = new TableInitializationContextImpl(
                    tablePath.getFileName().toString(),
                    tablePath.getParent(),
                    new TableIndex()
            );
            InitializationContext initializationContext = InitializationContextImpl.builder()
                    .executionEnvironment(env)
                    .currentDatabaseContext(dbContext)
                    .currentTableContext(tableContext)
                    .build();
            tableInitializer.perform(initializationContext);
        }

        env.addDatabase(DatabaseImpl.initializeFromContext(dbContext));
    }
}