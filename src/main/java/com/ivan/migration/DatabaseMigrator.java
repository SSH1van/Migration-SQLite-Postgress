package com.ivan.migration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class DatabaseMigrator {
    private static final Logger LOGGER = Logger.getLogger(DatabaseMigrator.class.getName());
    private Connection postgresConn;
    private final String postgresUrl;
    private final String postgresUser;
    private final String postgresPassword;
    private final String sourceFolderPath;

    // Кэш для хранения ID категорий
    private final Map<String, Integer> categoryCache = new HashMap<>();

    public DatabaseMigrator() {
        // Загружаем свойства из application.properties
        Properties props = loadProperties();
        this.postgresUrl = props.getProperty("postgres.url");
        this.postgresUser = props.getProperty("postgres.user");
        this.postgresPassword = props.getProperty("postgres.password");
        this.sourceFolderPath = props.getProperty("source.folder.path");
    }

    private Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Не удалось найти application.properties");
            }
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Ошибка загрузки application.properties", e);
        }
        return props;
    }

    public void migrate() throws SQLException {
        postgresConn = DriverManager.getConnection(postgresUrl, postgresUser, postgresPassword);

        try {
            // Обрабатываем все папки в директории results
            File rootFolder = new File(sourceFolderPath);
            File[] dateFolders = rootFolder.listFiles(File::isDirectory);

            if (dateFolders != null) {
                for (File folder : dateFolders) {
                    processFolder(folder);
                }
            }
        } catch (Exception e) {
            postgresConn.rollback();
            throw e;
        } finally {
            postgresConn.close();
        }
    }

    private void processFolder(File folder) throws SQLException {
        String sqliteDbPath = folder.getAbsolutePath() + File.separator + "products.db";
        String folderName = folder.getName();

        // Парсим дату из имени папки
        Timestamp date = parseDateFromFolderName(folderName);

        try (Connection sqliteConn = DriverManager.getConnection("jdbc:sqlite:" + sqliteDbPath)) {
            DatabaseMetaData meta = sqliteConn.getMetaData();
            ResultSet tables = meta.getTables(null, null, null, new String[]{"TABLE"});

            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                if (!tableName.equals("sqlite_sequence")) {
                    processTable(sqliteConn, tableName, date);
                }
            }
        }
    }

    private void processTable(Connection sqliteConn, String tableName, Timestamp date)
            throws SQLException {
        // Получаем или создаем категорию
        int categoryId = getOrCreateCategory(tableName);

        String selectSql = "SELECT price, link FROM \"" + tableName + "\"";
        try (Statement stmt = sqliteConn.createStatement();
             ResultSet rs = stmt.executeQuery(selectSql)) {

            while (rs.next()) {
                int price = rs.getInt("price");
                String url = rs.getString("link");

                // Получаем или создаем продукт
                long productId = getOrCreateProduct(url, categoryId);

                // Добавляем цену
                insertPrice(productId, date, price);
            }
        }
    }

    private int getOrCreateCategory(String name) throws SQLException {
        // Проверяем кэш
        if (categoryCache.containsKey(name)) {
            return categoryCache.get(name);
        }

        // Проверяем существование категории
        String checkSql = "SELECT id FROM categories WHERE name = ?";
        try (PreparedStatement pstmt = postgresConn.prepareStatement(checkSql)) {
            pstmt.setString(1, name);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                int id = rs.getInt("id");
                categoryCache.put(name, id);
                return id;
            }
        }

        // Создаем новую категорию
        String insertSql = "INSERT INTO categories (name) VALUES (?) RETURNING id";
        try (PreparedStatement pstmt = postgresConn.prepareStatement(insertSql)) {
            pstmt.setString(1, name);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            int id = rs.getInt("id");
            categoryCache.put(name, id);
            return id;
        }
    }

    private long getOrCreateProduct(String url, int categoryId) throws SQLException {
        // Проверяем существование продукта
        String checkSql = "SELECT id FROM products WHERE url = ?";
        try (PreparedStatement pstmt = postgresConn.prepareStatement(checkSql)) {
            pstmt.setString(1, url);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getLong("id");
            }
        }

        // Создаем новый продукт
        String insertSql = "INSERT INTO products (url, category_id) VALUES (?, ?) RETURNING id";
        try (PreparedStatement pstmt = postgresConn.prepareStatement(insertSql)) {
            pstmt.setString(1, url);
            pstmt.setInt(2, categoryId);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            return rs.getLong("id");
        }
    }

    private void insertPrice(long productId, Timestamp date, int price) throws SQLException {
        String insertSql = "INSERT INTO product_prices (product_id, date, price) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = postgresConn.prepareStatement(insertSql)) {
            pstmt.setLong(1, productId);
            pstmt.setTimestamp(2, date);
            pstmt.setInt(3, price);
            pstmt.executeUpdate();
        }
    }

    private Timestamp parseDateFromFolderName(String folderName) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
            java.util.Date date = sdf.parse(folderName);
            return new Timestamp(date.getTime());
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse date from folder name: " + folderName, e);
        }
    }

    public static void main(String[] args) {
        try {
            DatabaseMigrator migrator = new DatabaseMigrator();
            migrator.migrate();

            System.out.println("Миграция успешно выполнена");
        } catch (Exception e) {
            LOGGER.severe("Ошибка при выполнении миграции: " + e.getMessage());
        }
    }
}
