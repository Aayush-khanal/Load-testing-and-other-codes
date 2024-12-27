CREATE TABLE carrier_info (
    cid INT PRIMARY KEY AUTO_INCREMENT,
    carrier_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);