

# Drop Legacy Tables

DROP TABLE IF EXISTS `Time`;

# Dropping Existing Tables

DROP TABLE IF EXISTS ProductOrderLines;
DROP TABLE IF EXISTS ProductOrders;
DROP TABLE IF EXISTS SupplyOrderLines;
DROP TABLE IF EXISTS Supplies;
DROP TABLE IF EXISTS SupplyOrders;
DROP TABLE IF EXISTS TimecardLines;
DROP TABLE IF EXISTS Distributors;
DROP TABLE IF EXISTS Wines;
DROP TABLE IF EXISTS Employees;
DROP TABLE IF EXISTS Suppliers;

# Creating New Tables

#Core Entity Tables
CREATE TABLE Suppliers (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL
);

CREATE TABLE Employees (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL
);

CREATE TABLE Distributors (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL
);

CREATE TABLE Wines (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Available INT NOT NULL CHECK(Available >= 0)
);

#----------------------------------------------------------------------

# Tables with Foreign Keys
CREATE TABLE TimecardLines (
    EmployeeId INT NOT NULL,
    DateWorked DATE NOT NULL,
    HoursWorked DECIMAL(4, 2) NOT NULL CHECK (HoursWorked > 0 AND HoursWorked <= 24),
    PRIMARY KEY (EmployeeId, DateWorked),
    FOREIGN KEY (EmployeeId) REFERENCES Employees(Id)
);

CREATE TABLE Supplies (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Quantity INT NOT NULL CHECK (Quantity >= 0),
    PreferredSupplier INT NOT NULL,
    FOREIGN KEY (PreferredSupplier) REFERENCES Suppliers(Id)
);

CREATE TABLE SupplyOrders (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    SupplierId INT NOT NULL,
    ExpectedDate DATE NOT NULL,
    DeliveredDate DATE,
    FOREIGN KEY (SupplierId) REFERENCES Suppliers(Id)
);

CREATE TABLE ProductOrders (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    DistributorId INT NOT NULL,
    DatePlaced DATE NOT NULL,
    Carrier VARCHAR(100),
    TrackingNumber VARCHAR(255),
    FOREIGN KEY (DistributorId) REFERENCES Distributors(Id)
);

#----------------------------------------------------------------------

#Junction/Line-Item Tables
CREATE TABLE SupplyOrderLines (
    OrderId INT NOT NULL,
    SupplyId INT NOT NULL,
    Quantity INT NOT NULL CHECK (Quantity > 0),
    PRIMARY KEY (OrderId, SupplyId),
    FOREIGN KEY (OrderId) REFERENCES SupplyOrders(Id),
    FOREIGN KEY (SupplyId) REFERENCES Supplies(Id)
);

CREATE TABLE ProductOrderLines (
    OrderId INT NOT NULL,
    WineId INT NOT NULL,
    Quantity INT NOT NULL CHECK (Quantity > 0),

    PRIMARY KEY (OrderId, WineId),
    FOREIGN KEY (OrderId) REFERENCES ProductOrders(Id),
    FOREIGN KEY (WineId) REFERENCES Wines(Id)
);

#----------------------------------------------------------------------

# data insertion scripts

# Inserting Initial Data into Suppliers Table
INSERT INTO Suppliers (Id, Name) VALUES
(101,'Bottle & Cork Co.'),
(102, 'Label & Box Ltd.'),
(103, 'Vats & Tubing Inc.');

# Inserting Initial Data into Employees Table
INSERT INTO Employees (Id, Name) VALUES
(1, 'David Bacchus'),
(2, 'Stan Bacchus'),
(3, 'Janet Collins'),
(4, 'Roz Murphy'),
(5, 'Bob Ulrich'),
(6, 'Henry Doyle'),
(7, 'Maria Costanza');

# Inserting Initial Data into Distributors Table
INSERT INTO Distributors (Id, Name) VALUES
(301, "Southern Glazer’s Wine & Spirits"),
(302, 'Republic National Distributing Company'),
(303, 'Breakthru Beverage Group'),
(304, 'Winebow'),
(305, 'Johnson Brothers'),
(306, 'Young’s Market Company');

# Inserting Initial Data into Wines Table
INSERT INTO Wines (Id, Name, Available) VALUES
(401, 'Merlot', 300),
(402, 'Cabernet', 250),
(403, 'Chablis', 180),
(404, 'Chardonnay', 220);

# Inserting Initial Data into TimeCardLines Table
INSERT INTO TimecardLines (EmployeeId, DateWorked, HoursWorked) VALUES
(1, '2025-11-18', 8),
(2, '2025-11-18', 7),
(3, '2025-11-19', 8.5),
(4, '2025-11-19', 9),
(5, '2025-11-20', 6.5),
(6, '2025-11-20', 9.5),
(7, '2025-11-21', 8);

# Inserting Initial Data into Supplies Table
INSERT INTO Supplies (Id, Name, Quantity, PreferredSupplier) VALUES
(201, 'Wine bottles', 300, 101),
(202, 'Corks', 200, 101),
(203, 'Labels', 500, 102),
(204, 'Cardboard boxes', 400, 102),
(205, 'Vats', 3, 103),
(206, 'Tubing', 250, 103);

# Inserting Initial Data into SupplyOrders Table
INSERT INTO SupplyOrders (Id, SupplierId, ExpectedDate, DeliveredDate) VALUES
(5001,101,'2025-11-10','2025-11-9'),
(5002,102,'2025-11-12','2025-11-16'),
(5003,103,'2025-11-20','2025-11-20');

# Inserting Initial Data into ProductOrders Table
INSERT INTO ProductOrders (Id, DistributorId, DatePlaced, Carrier, TrackingNumber) VALUES
(6001,301,'2025-11-18','UPS','1Z-SUN-001'),
(6002,302,'2025-11-19','FedEx','FDX-COAST-002'),
(6003,303,'2025-11-20','UPS','1Z-MID-003'),
(6004,304,'2025-11-21','USPS','USPS-MTN-004'),
(6005,305,'2025-11-22','DHL','DHL-DESERT-005'),
(6006,306,'2025-11-23','FedEx','FDX-ATL-006');

# Inserting Initial Data into SupplyOrderLines Table
INSERT INTO SupplyOrderLines (OrderId, SupplyId, Quantity) VALUES
(5001,201,100),
(5001,202,50),
(5002,203,200),
(5002,204,150),
(5003,205,1),
(5003,206,100);

# Inserting Initial Data into ProductOrderLines Table
INSERT INTO ProductOrderLines (OrderId, WineId, Quantity) VALUES
(6001,401,50),
(6002,402,40),
(6003,404,60),
(6004,404,30),
(6005,401,25),
(6006,402,35),
(6001,403,60),
(6002,401,40),
(6003,403,20),
(6004,403,35),
(6005,404,25),
(6006,401,40);
-- BacchusWinery Database Schema Setup Complete --
