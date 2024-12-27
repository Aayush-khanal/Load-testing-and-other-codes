-- Table: plans
CREATE TABLE plan_policies (
    pid INT AUTO_INCREMENT PRIMARY KEY,
    policy_num INT NOT NULL,
    plan_id INT NOT NULL,
    pterm_date DATE NOT NULL,
    peffective_date DATE,
    pstatus ENUM('1', '2', '3') NOT NULL COMMENT '1 for active, 2 for termed, 3 for withdrawn',
    created_at TIMESTAMP NOT NULL COMMENT 'Unix timestamp for record creation',
    updated_at TIMESTAMP NOT NULL COMMENT 'Unix timestamp for last update',
    FOREIGN KEY (policy_num) REFERENCES policies(policy_id),
    FOREIGN KEY (plan_id) REFERENCES plans(pid)
);

