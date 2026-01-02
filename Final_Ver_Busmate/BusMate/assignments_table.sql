-- User-Bus Assignment Table
-- This table creates a many-to-many relationship between users and buses
-- A user can be assigned to multiple buses, and a bus can have multiple users assigned to it

CREATE TABLE IF NOT EXISTS `user_bus_assignment` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `userSID` INT NOT NULL,
  `busSID` INT NOT NULL,
  `assignedDate` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `unique_assignment` (`userSID`, `busSID`),
  FOREIGN KEY (`userSID`) REFERENCES `user`(`userSID`) ON DELETE CASCADE,
  FOREIGN KEY (`busSID`) REFERENCES `bus`(`busSID`) ON DELETE CASCADE,
  INDEX `idx_userSID` (`userSID`),
  INDEX `idx_busSID` (`busSID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Sample data (optional - add after table is created)
-- INSERT INTO `user_bus_assignment` (userSID, busSID) VALUES
-- (1, 1),
-- (1, 2),
-- (2, 1),
-- (3, 3);
