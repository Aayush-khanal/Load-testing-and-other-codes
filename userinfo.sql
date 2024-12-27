-- Table: userinfo
CREATE TABLE userinfo (
    userid INT AUTO_INCREMENT PRIMARY KEY,
    cfname VARCHAR(50) NOT NULL,
    clname VARCHAR(50) NOT NULL,
    cmname VARCHAR(50),
    cemail VARCHAR(100) NOT NULL,
    cgender ENUM('0', '1') COMMENT '1 for female, 0 for male',
    cdob DATE,
    cssn VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
