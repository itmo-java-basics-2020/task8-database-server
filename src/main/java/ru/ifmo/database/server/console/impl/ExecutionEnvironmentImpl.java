package ru.ifmo.database.server.console.impl;

import ru.ifmo.database.server.console.ExecutionEnvironment;
import ru.ifmo.database.server.logic.Database;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private static final String DEFAULT_DATABASE_FILES_DIRECTORY_NAME = "db_files";

    private final Map<String, Database> dbs = new HashMap<>();
    private final Path workingPath;

    public ExecutionEnvironmentImpl() {
        this(Path.of("", DEFAULT_DATABASE_FILES_DIRECTORY_NAME));
    }

    public ExecutionEnvironmentImpl(Path workingPath) {
        if (!Files.exists(workingPath)) {
            try {
                Files.createDirectory(workingPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.workingPath = Objects.requireNonNull(workingPath);
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        return Optional.ofNullable(dbs.get(name));
    }

    @Override
    public void addDatabase(Database db) {
        dbs.put(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return workingPath;
    }
}
