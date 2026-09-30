-- Run this file once as a MySQL administrator.
-- The application itself uses parameterized statements for all event writes.

CREATE DATABASE IF NOT EXISTS smartmeter
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'meter_app'@'localhost' IDENTIFIED BY 'meter_pass';
GRANT ALL PRIVILEGES ON smartmeter.* TO 'meter_app'@'localhost';
FLUSH PRIVILEGES;

USE smartmeter;

CREATE TABLE IF NOT EXISTS meter_readings (
    reading_pk BIGINT AUTO_INCREMENT PRIMARY KEY,
    reading_time DATETIME NOT NULL,
    global_active_power DECIMAL(10, 3) NOT NULL,
    rolling_avg_30 DECIMAL(10, 3) NOT NULL,
    is_anomaly TINYINT NOT NULL DEFAULT 0,
    anomaly_score DECIMAL(12, 8) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_meter_readings_time (reading_time),
    INDEX idx_meter_readings_anomaly (is_anomaly)
);

CREATE TABLE IF NOT EXISTS meter_metrics (
    metrics_pk TINYINT PRIMARY KEY,
    current_power DECIMAL(10, 3) NOT NULL DEFAULT 0,
    rolling_avg DECIMAL(10, 3) NOT NULL DEFAULT 0,
    peak_power DECIMAL(10, 3) NOT NULL DEFAULT 0,
    total_energy DECIMAL(14, 6) NOT NULL DEFAULT 0,
    anomaly_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO meter_metrics
    (metrics_pk, current_power, rolling_avg, peak_power, total_energy, anomaly_count)
VALUES (1, 0, 0, 0, 0, 0)
ON DUPLICATE KEY UPDATE metrics_pk = metrics_pk;
