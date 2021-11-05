package net.coreprotect.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;

import net.coreprotect.config.Config;
import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.consumer.Queue;
import net.coreprotect.consumer.process.Process;
import net.coreprotect.language.Phrase;
import net.coreprotect.model.BlockGroup;
import net.coreprotect.utility.Chat;
import net.coreprotect.utility.Color;
import net.coreprotect.utility.Util;

public class MySQLDatabase extends Database {

    public static final int SIGN = 0;
    public static final int BLOCK = 1;
    public static final int SKULL = 2;
    public static final int CONTAINER = 3;
    public static final int WORLD = 4;
    public static final int CHAT = 5;
    public static final int COMMAND = 6;
    public static final int SESSION = 7;
    public static final int ENTITY = 8;
    public static final int MATERIAL = 9;
    public static final int ART = 10;
    public static final int ENTITY_MAP = 11;
    public static final int BLOCKDATA = 12;
    public static final int ITEM = 13;

    public void beginTransaction(Statement statement) {
        Consumer.transacting = true;

        try {
            statement.executeUpdate("START TRANSACTION");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void commitTransaction(Statement statement) throws Exception {
        while (true) {
            try {
                statement.executeUpdate("COMMIT");
            } catch (Exception e) {
                e.printStackTrace();
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
            try {
                /*
                 * Using useServerPrepStmts, cachePrepStmts, and rewriteBatchedStatements per
                 * https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration
                 */
                String database = "jdbc:mysql://" + ConfigHandler.host + ":" + ConfigHandler.port + "/"
                        + ConfigHandler.database
                        + "?useUnicode=true&characterEncoding=utf-8&connectTimeout=10000&useSSL=false&allowPublicKeyRetrieval=true&useCursorFetch=true&useLocalSessionState=true&rewriteBatchedStatements=true&maintainTimeStats=false";
                connection = DriverManager.getConnection(database, ConfigHandler.username, ConfigHandler.password);

                /*
                 * Recommended implementation per
                 * https://dev.mysql.com/doc/refman/5.0/en/charset-applications.html &
                 * https://dev.mysql.com/doc/refman/5.0/en/charset-syntax.html
                 */
                Statement statement = connection.createStatement();
                statement.executeUpdate("SET NAMES 'utf8mb4'"); // COLLATE 'utf8mb4mb4_general_ci'
                statement.close();

                ConfigHandler.databaseReachable = true;
            } catch (Exception e) {
                ConfigHandler.databaseReachable = false;
                Chat.sendConsoleMessage(Color.RED + "[CoreProtect] " + Phrase.build(Phrase.MYSQL_UNAVAILABLE));
                e.printStackTrace();
            }

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

    public void initializeTables(String prefix, Statement statement) {
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

        if (Config.getGlobal().MYSQL) {
            boolean success = false;
            try {
                Connection connection = this.getConnection(true, true, true, 0);
                if (connection != null) {
                    String index = "";
                    Statement statement = connection.createStatement();
                    index = ", INDEX(id)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "art_map(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),id int(8),art varchar(255)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(wid,x,z,time), INDEX(user,time), INDEX(type,time)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "block(rowid bigint(20) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid), time int(10), user int(8), wid int(4), x int(8), y int(3), z int(8), type int(6), data int(8), meta mediumblob, blockdata blob, action int(2), rolled_back tinyint(1)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(time), INDEX(user,time), INDEX(wid,x,z,time)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "chat(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),time int(10), user int(8), wid int(4), x int(8), y int (3), z int(8), message varchar(16000)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(time), INDEX(user,time), INDEX(wid,x,z,time)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "command(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),time int(10), user int(8), wid int(4), x int(8), y int (3), z int(8), message varchar(16000)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(wid,x,z,time), INDEX(user,time), INDEX(type,time)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "container(rowid int(10) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid), time int(10), user int(8), wid int(4), x int(8), y int(3), z int(8), type int(6), data int(6), amount int(4), metadata blob, action int(2), rolled_back tinyint(1)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(wid,x,z,time), INDEX(user,time), INDEX(type,time)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "item(rowid int(10) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid), time int(10), user int(8), wid int(4), x int(8), y int(3), z int(8), type int(6), data blob, amount int(4), action tinyint(1)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "database_lock(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),status tinyint(1),time int(10)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "entity(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid), time int(10), data blob) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(id)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "entity_map(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),id int(8),entity varchar(255)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(id)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "material_map(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),id int(8),material varchar(255)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(id)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "blockdata_map(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),id int(8),data varchar(255)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(wid,x,z,time), INDEX(action,time), INDEX(user,time), INDEX(time)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "session(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),time int(10), user int(8), wid int(4), x int(8), y int (3), z int(8), action int(1)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(wid,x,z,time), INDEX(user,time), INDEX(time)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "sign(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),time int(10), user int(8), wid int(4), x int(8), y int(3), z int(8), action tinyint(1), color int(8), data tinyint(1), line_1 varchar(100), line_2 varchar(100), line_3 varchar(100), line_4 varchar(100)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "skull(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid), time int(10), owner varchar(64)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(user), INDEX(uuid)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "user(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),time int(10),user varchar(100),uuid varchar(64)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(uuid,user)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "username_log(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),time int(10),uuid varchar(64),user varchar(100)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "version(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),time int(10),version varchar(16)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    index = ", INDEX(id)";
                    statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + prefix
                            + "world(rowid int(8) NOT NULL AUTO_INCREMENT,PRIMARY KEY(rowid),id int(8),world varchar(255)"
                            + index + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4");
                    if (!purge) {
                        initializeTables(prefix, statement);
                    }
                    statement.close();
                    connection.close();
                    success = true;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (!success) {
                Config.getGlobal().MYSQL = false;
            }
        }
    }

}
