package ru.ifmo.database.server.console;

import ru.ifmo.database.server.console.impl.CreateDatabaseCommand;
import ru.ifmo.database.server.console.impl.CreateTableCommand;
import ru.ifmo.database.server.console.impl.ReadKeyCommand;
import ru.ifmo.database.server.console.impl.UpdateKeyCommand;
import ru.ifmo.database.server.logic.impl.DatabaseImpl;

public enum DatabaseCommands {

    CREATE_DATABASE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args)
        {
            if(args.length != 1)
                return () -> DatabaseCommandResult.error("Not correct amount of arguments");

            return new CreateDatabaseCommand(env, DatabaseImpl::create, args[0]);
        }
    },

    CREATE_TABLE {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args)
        {
            if(args.length != 2)
                return () -> DatabaseCommandResult.error("Not correct amount of arguments");

            return new CreateTableCommand(env, args);
        }
    },

    UPDATE_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args)
        {
            if(args.length != 4)
                return () -> DatabaseCommandResult.error("Not correct amount of arguments");

            return new UpdateKeyCommand(env, args);
        }
    },

    READ_KEY {
        @Override
        public DatabaseCommand getCommand(ExecutionEnvironment env, String... args)
        {
            if(args.length != 3)
                return () -> DatabaseCommandResult.error("Not correct amount of arguments");

            return new ReadKeyCommand(env, args);
        }
    };

    public abstract DatabaseCommand getCommand(ExecutionEnvironment env, String... args);
}


