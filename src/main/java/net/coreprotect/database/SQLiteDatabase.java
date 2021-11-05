package net.coreprotect.database;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.consumer.Queue;
import net.coreprotect.consumer.process.Process;
import net.coreprotect.database.statement.UserStatement;
import net.coreprotect.language.Phrase;
import net.coreprotect.model.BlockGroup;
import net.coreprotect.utility.Chat;
import net.coreprotect.utility.Util;

public class SQLiteDatabase extends Database {

    public void beginTransaction(Statement statement) {
        Consumer.transacting = true;

        try {
            statement.executeUpdate("BEGIN TRANSACTION");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void commitTransaction(Statement statement) throws Exception {
        int count = 0;

        while (true) {
            try {
                statement.executeUpdate("COMMIT TRANSACTION");
            } catch (Exception e) {
                if (e.getMessage().startsWith("[SQLITE_BUSY]") && count < 30) {
                    Thread.sleep(1000);
                    count++;

                    continue;
                } else {
                    e.printStackTrace();
                }
            }

            Consumer.transacting = false;
            Consumer.interrupt = false;
            return;
        }
    }

    public void setMultiInt(PreparedStatement statement, int value, int count) {
        try {
            for (int i = 1; i <= count; i++) {
                statement.setInt(i, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void containerBreakCheck(String user, Material type, Object container, ItemStack[] contents,
            Location location) {
        if (BlockGroup.CONTAINERS.contains(type) && !BlockGroup.SHULKER_BOXES.contains(type)) {
            if (Config.getConfig(location.getWorld()).ITEM_TRANSACTIONS) {
                try {
                    if (contents == null) {
                        contents = Util.getContainerContents(type, container, location);
                    }
                    if (contents != null) {
                        List<ItemStack[]> forceList = new ArrayList<>();
                        forceList.add(Util.getContainerState(contents));
                        ConfigHandler.forceContainer.put(user.toLowerCase(Locale.ROOT) + "." + location.getBlockX()
                                + "." + location.getBlockY() + "." + location.getBlockZ(), forceList);
                        Queue.queueContainerBreak(user, location, type, contents);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public Connection getConnection(boolean onlyCheckTransacting) {
        // Previously 250ms; long consumer commit time may be due to batching
        // TODO: Investigate, potentially remove batching for SQLite connections
        return getConnection(false, false, onlyCheckTransacting, 1000);
    }

    public Connection getConnection(boolean force, int waitTime) {
        return getConnection(force, false, false, waitTime);
    }

    public Connection getConnection(boolean force, boolean startup, boolean onlyCheckTransacting, int waitTime) {
        Connection connection = null;
        try {
            if (!force && (ConfigHandler.converterRunning || ConfigHandler.purgeRunning)) {
                return connection;
            }

            if (Consumer.transacting && onlyCheckTransacting) {
                Consumer.interrupt = true;
            }

            long startTime = System.nanoTime();
            while (Consumer.isPaused && !force && (Consumer.transacting || !onlyCheckTransacting)) {
                Thread.sleep(1);
                long pauseTime = (System.nanoTime() - startTime) / 1000000;

                if (pauseTime >= waitTime) {
                    return connection;
                }
            }

            String database = "jdbc:sqlite:" + ConfigHandler.path + ConfigHandler.sqliteDatabase + "";
            connection = DriverManager.getConnection(database);

            ConfigHandler.databaseReachable = true;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return connection;
    }

    public void performUpdate(Statement statement, long id, int action, int table) {
        try {
            int rolledBack = 1;
            if (action == 1) {
                rolledBack = 0;
            }

            if (table == 1) {
                statement.executeUpdate("UPDATE " + ConfigHandler.prefix + "container SET rolled_back='" + rolledBack
                        + "' WHERE rowid='" + id + "'");
            } else {
                statement.executeUpdate("UPDATE " + ConfigHandler.prefix + "block SET rolled_back='" + rolledBack
                        + "' WHERE rowid='" + id + "'");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public PreparedStatement prepareStatement(Connection connection, int type, boolean keys) {
        PreparedStatement preparedStatement = null;
        try {
            String signInsert = "INSERT INTO " + ConfigHandler.prefix
                    + "sign (time, user, wid, x, y, z, action, color, data, line_1, line_2, line_3, line_4) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            String blockInsert = "INSERT INTO " + ConfigHandler.prefix
                    + "block (time, user, wid, x, y, z, type, data, meta, blockdata, action, rolled_back) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            String skullInsert = "INSERT INTO " + ConfigHandler.prefix + "skull (time, owner) VALUES (?, ?)";
            String containerInsert = "INSERT INTO " + ConfigHandler.prefix
                    + "container (time, user, wid, x, y, z, type, data, amount, metadata, action, rolled_back) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            String itemInsert = "INSERT INTO " + ConfigHandler.prefix
                    + "item (time, user, wid, x, y, z, type, data, amount, action) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            String worldInsert = "INSERT INTO " + ConfigHandler.prefix + "world (id, world) VALUES (?, ?)";
            String chatInsert = "INSERT INTO " + ConfigHandler.prefix
                    + "chat (time, user, wid, x, y, z, message) VALUES (?, ?, ?, ?, ?, ?, ?)";
            String commandInsert = "INSERT INTO " + ConfigHandler.prefix
                    + "command (time, user, wid, x, y, z, message) VALUES (?, ?, ?, ?, ?, ?, ?)";
            String sessionInsert = "INSERT INTO " + ConfigHandler.prefix
                    + "session (time, user, wid, x, y, z, action) VALUES (?, ?, ?, ?, ?, ?, ?)";
            String entityInsert = "INSERT INTO " + ConfigHandler.prefix + "entity (time, data) VALUES (?, ?)";
            String materialInsert = "INSERT INTO " + ConfigHandler.prefix + "material_map (id, material) VALUES (?, ?)";
            String artInsert = "INSERT INTO " + ConfigHandler.prefix + "art_map (id, art) VALUES (?, ?)";
            String entityMapInsert = "INSERT INTO " + ConfigHandler.prefix + "entity_map (id, entity) VALUES (?, ?)";
            String blockdataInsert = "INSERT INTO " + ConfigHandler.prefix + "blockdata_map (id, data) VALUES (?, ?)";

            switch (type) {
            case SIGN:
                preparedStatement = prepareStatement(connection, signInsert, keys);
                break;
            case BLOCK:
                preparedStatement = prepareStatement(connection, blockInsert, keys);
                break;
            case SKULL:
                preparedStatement = prepareStatement(connection, skullInsert, keys);
                break;
            case CONTAINER:
                preparedStatement = prepareStatement(connection, containerInsert, keys);
                break;
            case ITEM:
                preparedStatement = prepareStatement(connection, itemInsert, keys);
                break;
            case WORLD:
                preparedStatement = prepareStatement(connection, worldInsert, keys);
                break;
            case CHAT:
                preparedStatement = prepareStatement(connection, chatInsert, keys);
                break;
            case COMMAND:
                preparedStatement = prepareStatement(connection, commandInsert, keys);
                break;
            case SESSION:
                preparedStatement = prepareStatement(connection, sessionInsert, keys);
                break;
            case ENTITY:
                preparedStatement = prepareStatement(connection, entityInsert, keys);
                break;
            case MATERIAL:
                preparedStatement = prepareStatement(connection, materialInsert, keys);
                break;
            case ART:
                preparedStatement = prepareStatement(connection, artInsert, keys);
                break;
            case ENTITY_MAP:
                preparedStatement = prepareStatement(connection, entityMapInsert, keys);
                break;
            case BLOCKDATA:
                preparedStatement = prepareStatement(connection, blockdataInsert, keys);
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return preparedStatement;
    }

    public PreparedStatement prepareStatement(Connection connection, String query, boolean keys) {
        PreparedStatement preparedStatement = null;
        try {
            if (keys) {
                preparedStatement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            } else {
                preparedStatement = connection.prepareStatement(query);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return preparedStatement;
    }

    protected void initializeTables(String prefix, Statement statement) {
        try {
            boolean lockInitialized = false;
            String query = "SELECT rowid as id FROM " + prefix + "database_lock WHERE rowid='1' LIMIT 1";
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                lockInitialized = true;
            }
            rs.close();

            if (!lockInitialized) {
                int unixtimestamp = (int) (System.currentTimeMillis() / 1000L);
                statement.executeUpdate("INSERT INTO " + prefix
                        + "database_lock (rowid, status, time) VALUES ('1', '0', '" + unixtimestamp + "')");
                Process.lastLockUpdate = 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createDatabaseTables(String prefix, boolean purge) {
        ConfigHandler.databaseTables.clear();
        ConfigHandler.databaseTables.addAll(Arrays.asList("art_map", "block", "chat", "command", "container", "item",
                "database_lock", "entity", "entity_map", "material_map", "blockdata_map", "session", "sign", "skull",
                "user", "username_log", "version", "world"));

        try {
            Connection connection = this.getConnection(true, 0);
            Statement statement = connection.createStatement();
            List<String> tableData = new ArrayList<>();
            List<String> indexData = new ArrayList<>();
            String attachDatabase = "";

            if (purge) {
                String query = "ATTACH DATABASE '" + ConfigHandler.path + ConfigHandler.sqliteDatabase + ".tmp' AS tmp_db";
                PreparedStatement preparedStmt = connection.prepareStatement(query);
                preparedStmt.execute();
                preparedStmt.close();
                attachDatabase = "tmp_db.";
            }

            String query = "SELECT type,name FROM " + attachDatabase
                    + "sqlite_master WHERE type='table' OR type='index';";
            ResultSet rs = statement.executeQuery(query);
            while (rs.next()) {
                String type = rs.getString("type");
                if (type.equalsIgnoreCase("table")) {
                    tableData.add(rs.getString("name"));
                } else if (type.equalsIgnoreCase("index")) {
                    indexData.add(rs.getString("name"));
                }
            }
            rs.close();

            if (!tableData.contains(prefix + "art_map")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix + "art_map (id INTEGER, art TEXT);");
            }
            if (!tableData.contains(prefix + "block")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                        + "block (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, meta BLOB, blockdata BLOB, action INTEGER, rolled_back INTEGER);");
            }
            if (!tableData.contains(prefix + "chat")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                        + "chat (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, message TEXT);");
            }
            if (!tableData.contains(prefix + "command")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                        + "command (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, message TEXT);");
            }
            if (!tableData.contains(prefix + "container")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                        + "container (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data INTEGER, amount INTEGER, metadata BLOB, action INTEGER, rolled_back INTEGER);");
            }
            if (!tableData.contains(prefix + "item")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                        + "item (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, type INTEGER, data BLOB, amount INTEGER, action INTEGER);");
            }
            if (!tableData.contains(prefix + "database_lock")) {
                statement.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS " + prefix + "database_lock (status INTEGER, time INTEGER);");
            }
            if (!tableData.contains(prefix + "entity")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                        + "entity (id INTEGER PRIMARY KEY ASC, time INTEGER, data BLOB);");
            }
            if (!tableData.contains(prefix + "entity_map")) {
                statement.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS " + prefix + "entity_map (id INTEGER, entity TEXT);");
            }
            if (!tableData.contains(prefix + "material_map")) {
                statement.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS " + prefix + "material_map (id INTEGER, material TEXT);");
            }
            if (!tableData.contains(prefix + "blockdata_map")) {
                statement.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS " + prefix + "blockdata_map (id INTEGER, data TEXT);");
            }
            if (!tableData.contains(prefix + "session")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                        + "session (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, action INTEGER);");
            }
            if (!tableData.contains(prefix + "sign")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                        + "sign (time INTEGER, user INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, action INTEGER, color INTEGER, data INTEGER, line_1 TEXT, line_2 TEXT, line_3 TEXT, line_4 TEXT);");
            }
            if (!tableData.contains(prefix + "skull")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                        + "skull (id INTEGER PRIMARY KEY ASC, time INTEGER, owner TEXT);");
            }
            if (!tableData.contains(prefix + "user")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                        + "user (id INTEGER PRIMARY KEY ASC, time INTEGER, user TEXT, uuid TEXT);");
            }
            if (!tableData.contains(prefix + "username_log")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                        + "username_log (id INTEGER PRIMARY KEY ASC, time INTEGER, uuid TEXT, user TEXT);");
            }
            if (!tableData.contains(prefix + "version")) {
                statement.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS " + prefix + "version (time INTEGER, version TEXT);");
            }
            if (!tableData.contains(prefix + "world")) {
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix + "world (id INTEGER, world TEXT);");
            }
            try {
                if (!indexData.contains("art_map_id_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "art_map_id_index ON "
                            + ConfigHandler.prefix + "art_map(id);");
                }
                if (!indexData.contains("block_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "block_index ON "
                            + ConfigHandler.prefix + "block(wid,x,z,time);");
                }
                if (!indexData.contains("block_user_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "block_user_index ON "
                            + ConfigHandler.prefix + "block(user,time);");
                }
                if (!indexData.contains("block_type_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "block_type_index ON "
                            + ConfigHandler.prefix + "block(type,time);");
                }
                if (!indexData.contains("blockdata_map_id_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase
                            + "blockdata_map_id_index ON " + ConfigHandler.prefix + "blockdata_map(id);");
                }
                if (!indexData.contains("chat_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "chat_index ON "
                            + ConfigHandler.prefix + "chat(time);");
                }
                if (!indexData.contains("chat_user_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "chat_user_index ON "
                            + ConfigHandler.prefix + "chat(user,time);");
                }
                if (!indexData.contains("chat_wid_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "chat_wid_index ON "
                            + ConfigHandler.prefix + "chat(wid,x,z,time);");
                }
                if (!indexData.contains("command_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "command_index ON "
                            + ConfigHandler.prefix + "command(time);");
                }
                if (!indexData.contains("command_user_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "command_user_index ON "
                            + ConfigHandler.prefix + "command(user,time);");
                }
                if (!indexData.contains("command_wid_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "command_wid_index ON "
                            + ConfigHandler.prefix + "command(wid,x,z,time);");
                }
                if (!indexData.contains("container_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "container_index ON "
                            + ConfigHandler.prefix + "container(wid,x,z,time);");
                }
                if (!indexData.contains("container_user_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "container_user_index ON "
                            + ConfigHandler.prefix + "container(user,time);");
                }
                if (!indexData.contains("container_type_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "container_type_index ON "
                            + ConfigHandler.prefix + "container(type,time);");
                }
                if (!indexData.contains("item_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "item_index ON "
                            + ConfigHandler.prefix + "item(wid,x,z,time);");
                }
                if (!indexData.contains("item_user_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "item_user_index ON "
                            + ConfigHandler.prefix + "item(user,time);");
                }
                if (!indexData.contains("item_type_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "item_type_index ON "
                            + ConfigHandler.prefix + "item(type,time);");
                }
                if (!indexData.contains("entity_map_id_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "entity_map_id_index ON "
                            + ConfigHandler.prefix + "entity_map(id);");
                }
                if (!indexData.contains("material_map_id_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "material_map_id_index ON "
                            + ConfigHandler.prefix + "material_map(id);");
                }
                if (!indexData.contains("session_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "session_index ON "
                            + ConfigHandler.prefix + "session(wid,x,z,time);");
                }
                if (!indexData.contains("session_action_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "session_action_index ON "
                            + ConfigHandler.prefix + "session(action,time);");
                }
                if (!indexData.contains("session_user_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "session_user_index ON "
                            + ConfigHandler.prefix + "session(user,time);");
                }
                if (!indexData.contains("session_time_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "session_time_index ON "
                            + ConfigHandler.prefix + "session(time);");
                }
                if (!indexData.contains("sign_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "sign_index ON "
                            + ConfigHandler.prefix + "sign(wid,x,z,time);");
                }
                if (!indexData.contains("sign_user_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "sign_user_index ON "
                            + ConfigHandler.prefix + "sign(user,time);");
                }
                if (!indexData.contains("sign_time_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "sign_time_index ON "
                            + ConfigHandler.prefix + "sign(time);");
                }
                if (!indexData.contains("user_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "user_index ON "
                            + ConfigHandler.prefix + "user(user);");
                }
                if (!indexData.contains("uuid_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "uuid_index ON "
                            + ConfigHandler.prefix + "user(uuid);");
                }
                if (!indexData.contains("username_log_uuid_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase
                            + "username_log_uuid_index ON " + ConfigHandler.prefix + "username_log(uuid,user);");
                }
                if (!indexData.contains("world_id_index")) {
                    statement.executeUpdate("CREATE INDEX IF NOT EXISTS " + attachDatabase + "world_id_index ON "
                            + ConfigHandler.prefix + "world(id);");
                }
            } catch (Exception e) {
                Chat.console(Phrase.build(Phrase.DATABASE_INDEX_ERROR));
                if (purge) {
                    e.printStackTrace();
                }
            }
            if (!purge) {
                initializeTables(prefix, statement);
            }
            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void loadDatabase() {
        try {
            File tempFile = File.createTempFile("CoreProtect_" + System.currentTimeMillis(), ".tmp");
            tempFile.setExecutable(true);

            boolean canExecute = false;
            try {
                canExecute = tempFile.canExecute();
            } catch (Exception exception) {
                // execute access denied by security manager
            }

            if (!canExecute) {
                File tempFolder = new File("cache");
                boolean exists = tempFolder.exists();
                if (!exists) {
                    tempFolder.mkdir();
                }
                System.setProperty("java.io.tmpdir", "cache");
            }

            tempFile.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (ConfigHandler.serverRunning) {
            Consumer.resetConnection = true;
        }
    }

    @Override
    public void loadOnlinePlayers() {
        try (Connection connection = this.getConnection(true, 0); Statement statement = connection.createStatement()) {
            ConfigHandler.playerIdCache.clear();
            ConfigHandler.playerIdCacheReversed.clear();
            for (Player player : Bukkit.getServer().getOnlinePlayers()) {
                if (ConfigHandler.playerIdCache.get(player.getName().toLowerCase(Locale.ROOT)) == null) {
                    UserStatement.loadId(connection, player.getName(), player.getUniqueId().toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void loadTypes() {
        
    }

    @Override
    protected void loadWorlds() {
        
    }
}
