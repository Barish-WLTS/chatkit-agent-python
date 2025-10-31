-- Multi-Brand Chatbot Database Schema
-- Fully dynamic structure for future scalability

CREATE DATABASE IF NOT EXISTS chatbot_system CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE chatbot_system;

-- 1. BRANDS TABLE (Dynamic brand management)
CREATE TABLE brands (
    id INT AUTO_INCREMENT PRIMARY KEY,
    brand_key VARCHAR(50) UNIQUE NOT NULL,
    brand_display_name VARCHAR(100) NOT NULL,
    brand_email VARCHAR(255),
    logo_url VARCHAR(255),
    vector_store_id VARCHAR(100),
    agent_instructions TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_brand_key (brand_key),
    INDEX idx_active (is_active)
) ENGINE=InnoDB;

-- 2. USERS TABLE (Centralized user management)
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    phone VARCHAR(50),
    business_name VARCHAR(255),
    website VARCHAR(255),
    location VARCHAR(255),
    ip_address VARCHAR(50),
    city VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100),
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    total_conversations INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_email (email),
    INDEX idx_phone (phone),
    INDEX idx_last_seen (last_seen)
) ENGINE=InnoDB;

-- 3. SESSIONS TABLE (Conversation sessions)
CREATE TABLE sessions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    session_id VARCHAR(100) UNIQUE NOT NULL,
    user_id INT,
    brand_id INT NOT NULL,
    status ENUM('active', 'ended', 'timeout') DEFAULT 'active',
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP NULL,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    duration_seconds INT DEFAULT 0,
    message_count INT DEFAULT 0,
    user_message_count INT DEFAULT 0,
    assistant_message_count INT DEFAULT 0,
    email_sent BOOLEAN DEFAULT FALSE,
    email_sent_at TIMESTAMP NULL,
    contact_ask_count INT DEFAULT 0,
    total_input_tokens INT DEFAULT 0,
    total_output_tokens INT DEFAULT 0,
    total_tokens INT DEFAULT 0,
    last_input_tokens INT DEFAULT 0,
    last_output_tokens INT DEFAULT 0,
    last_token_usage INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
    FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
    INDEX idx_session_id (session_id),
    INDEX idx_user_id (user_id),
    INDEX idx_brand_id (brand_id),
    INDEX idx_status (status),
    INDEX idx_last_activity (last_activity),
    INDEX idx_email_sent (email_sent)
) ENGINE=InnoDB;

-- 4. MESSAGES TABLE (All conversation messages)
CREATE TABLE messages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    session_id INT NOT NULL,
    role ENUM('user', 'assistant', 'system') NOT NULL,
    content TEXT NOT NULL,
    content_type VARCHAR(50) DEFAULT 'text',
    formatted_content TEXT,
    file_name VARCHAR(255),
    file_size INT,
    input_tokens INT DEFAULT 0,
    output_tokens INT DEFAULT 0,
    total_tokens INT DEFAULT 0,
    message_order INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
    INDEX idx_session_id (session_id),
    INDEX idx_role (role),
    INDEX idx_message_order (message_order),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB;

-- 5. EMAIL_LOGS TABLE (Track all email sends)
CREATE TABLE email_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    session_id INT NOT NULL,
    user_id INT,
    brand_id INT NOT NULL,
    recipient_emails TEXT NOT NULL,
    subject VARCHAR(500),
    status ENUM('pending', 'sent', 'failed') DEFAULT 'pending',
    attempt_count INT DEFAULT 0,
    sent_at TIMESTAMP NULL,
    error_message TEXT,
    html_content LONGTEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
    FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
    INDEX idx_session_id (session_id),
    INDEX idx_status (status),
    INDEX idx_sent_at (sent_at)
) ENGINE=InnoDB;

-- 6. USER_BRAND_INTERACTIONS (Track user interactions per brand)
CREATE TABLE user_brand_interactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    brand_id INT NOT NULL,
    total_sessions INT DEFAULT 0,
    total_messages INT DEFAULT 0,
    total_emails_sent INT DEFAULT 0,
    first_interaction TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_interaction TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    total_input_tokens INT DEFAULT 0,
    total_output_tokens INT DEFAULT 0,
    total_tokens INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
    UNIQUE KEY unique_user_brand (user_id, brand_id),
    INDEX idx_user_id (user_id),
    INDEX idx_brand_id (brand_id),
    INDEX idx_last_interaction (last_interaction)
) ENGINE=InnoDB;

-- 7. BRAND_RECIPIENTS TABLE (Dynamic recipient management per brand)
CREATE TABLE brand_recipients (
    id INT AUTO_INCREMENT PRIMARY KEY,
    brand_id INT NOT NULL,
    email VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
    INDEX idx_brand_id (brand_id),
    INDEX idx_active (is_active)
) ENGINE=InnoDB;

-- 8. ANALYTICS_SUMMARY (Pre-computed analytics for dashboard)
CREATE TABLE analytics_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    brand_id INT,
    date DATE NOT NULL,
    total_sessions INT DEFAULT 0,
    total_messages INT DEFAULT 0,
    total_users INT DEFAULT 0,
    new_users INT DEFAULT 0,
    emails_sent INT DEFAULT 0,
    avg_session_duration FLOAT DEFAULT 0,
    avg_messages_per_session FLOAT DEFAULT 0,
    total_input_tokens INT DEFAULT 0,
    total_output_tokens INT DEFAULT 0,
    total_tokens INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (brand_id) REFERENCES brands(id) ON DELETE CASCADE,
    UNIQUE KEY unique_brand_date (brand_id, date),
    INDEX idx_brand_id (brand_id),
    INDEX idx_date (date)
) ENGINE=InnoDB;

-- Insert default brands
INSERT INTO brands (brand_key, brand_display_name, brand_email, vector_store_id, agent_instructions, is_active) VALUES
('gbpseo', 'GBPSEO', 'chatbot@gbpseo.in', 'vs_68e895ebfd088191ab82202452458820', 'GBP SEO specialist', TRUE),
('whitedigital', 'whiteDigital', 'chatbot@gbpseo.in', 'vs_68f61c986dec8191809bf8ce6ef8282f', 'PPC advertising specialist', TRUE);

-- Insert default recipients for GBPSEO
INSERT INTO brand_recipients (brand_id, email, is_active) VALUES
(1, 'barishwlts@gmail.com', TRUE);

-- Insert default recipients for WhiteDigital
INSERT INTO brand_recipients (brand_id, email, is_active) VALUES
(2, 'barishwlts@gmail.com', TRUE);

-- Create stored procedure for session cleanup (automatic timeout handling)
DELIMITER //
CREATE PROCEDURE cleanup_inactive_sessions()
BEGIN
    UPDATE sessions 
    SET status = 'timeout',
        ended_at = NOW(),
        duration_seconds = TIMESTAMPDIFF(SECOND, started_at, NOW())
    WHERE status = 'active' 
    AND last_activity < DATE_SUB(NOW(), INTERVAL 5 MINUTE);
END //
DELIMITER ;

-- Create event for automatic cleanup (runs every 5 minutes)
CREATE EVENT IF NOT EXISTS auto_cleanup_sessions
ON SCHEDULE EVERY 5 MINUTE
DO CALL cleanup_inactive_sessions();