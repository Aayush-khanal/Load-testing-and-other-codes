CREATE TABLE policies (
    policy_id INT PRIMARY KEY AUTO_INCREMENT,
    policy_userid INT NOT NULL,
    edate int, 
    term_date DATE, 
    effective_date DATE NOT NULL,
    status ENUM('ACTIVE', 'TERMED', 'WITHDRAWN') NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (policy_userid) REFERENCES userinfo(userid)
);

