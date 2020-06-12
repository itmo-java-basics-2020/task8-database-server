package ru.ifmo.database.server.console;

import ru.ifmo.database.server.console.impl.CreateDatabaseCommand;
import ru.ifmo.database.server.console.impl.CreateTableCommand;
import ru.ifmo.database.server.console.impl.ReadKeyCommand;
import ru.ifmo.database.server.console.impl.UpdateKeyCommand;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

public enum DatabaseCommands {

    CREATE_DATABASE {
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            if (args.length < 2) {
                throw new IllegalArgumentException("For CREATE_DATABASE command at least one arguments required: database name.");
            }
            return new CreateDatabaseCommand(env, DatabaseImpl::create, args[1]);
        }
    },
    CREATE_TABLE {
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            if (args.length < 3) {
                throw new IllegalArgumentException("For CREATE_TABLE command at least two arguments required: database name, table name.");
            }
            return new CreateTableCommand(env, args[1], args[2]);
        }
    },
    UPDATE_KEY {
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            if (args.length < 5) {
                throw new IllegalArgumentException("For UPDATE_KEY command at least fore arguments required: database name, table name, key, value.");
            }
            return new UpdateKeyCommand(env, args[1], args[2], args[3], args[4]);
        }
    },
    READ_KEY {
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            if (args.length < 4) {
                throw new IllegalArgumentException("For READ_KEY command at least three arguments required: database name, table name, key.");
            }
            return new ReadKeyCommand(env, args[1], args[2], args[3]);
        }
    };

    public abstract DatabaseCommand getCommand(ExecutionEnvironment env, String... args);

    /*
    CREATE_DATABASE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            return new CreateDatabaseCommand(env, DatabaseImpl::create, args);
        }
    },
    CREATE_TABLE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            return new CreateTableCommand(env, args);
        }
    },
    UPDATE_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            return new UpdateKeyCommand(env, args);
        }
    },
    READ_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args) {
            return new ReadKeyCommand(env, args);
        }
    };


    public abstract DatabaseCommand getCommand(ExecutionEnvironment env, String... args);
     */
}
