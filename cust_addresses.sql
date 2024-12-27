-- Table: userinfo
CREATE TABLE cust_addresses (
    address_id INT AUTO_INCREMENT PRIMARY KEY,
    a_userid INT NOT NULL,
    address1 VARCHAR(255) NOT NULL,
    address2 VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100),
    zip VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (a_userid) REFERENCES userinfo(userid)
);

