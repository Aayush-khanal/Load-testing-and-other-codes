CREATE TABLE plan_pricing (
    plan_pricing_id INT PRIMARY KEY AUTO_INCREMENT,
    plan_id INT NOT NULL,
    price_male_nons FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (plan_id) REFERENCES plan_tier(idplan_tier)
);