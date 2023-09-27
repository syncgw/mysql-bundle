-- 
-- 	MySQL table definition
--
--	@package	sync*gw
--	@subpackage	mySQLi handler
--	@copyright	(c) 2008 - 2023 Florian Daeumling, Germany. All right reserved
-- 	@license 	LGPL-3.0-or-later
--

-- User
DROP TABLE IF EXISTS `{prefix}_User`;
CREATE TABLE `{prefix}_User` (
  `Uid`		 	INT,
  `GUID`     	VARCHAR(64)		COLLATE utf8_general_ci,
  `LUID`     	VARCHAR(64) 	COLLATE utf8_general_ci,
  `Group`	 	VARCHAR(256) 	COLLATE utf8_general_ci,
  `Type`	 	VARCHAR(1) 		COLLATE utf8_general_ci,
  `SyncStat`	VARCHAR(3)		COLLATE utf8_general_ci,
  `XML`      	MEDIUMTEXT		COLLATE utf8_general_ci,
  PRIMARY KEY (`Uid`, `GUID`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

-- Session
DROP TABLE IF EXISTS `{prefix}_Session`;
CREATE TABLE `{prefix}_Session` (
  `Uid`		 	INT,
  `GUID`     	VARCHAR(64)		COLLATE utf8_general_ci,
  `LUID`     	VARCHAR(64) 	COLLATE utf8_general_ci,
  `Group`	 	VARCHAR(256) 	COLLATE utf8_general_ci,
  `Type`	 	VARCHAR(1) 		COLLATE utf8_general_ci,
  `SyncStat`	VARCHAR(3)		COLLATE utf8_general_ci,
  `XML`      	MEDIUMTEXT		COLLATE utf8_general_ci,
  PRIMARY KEY (`Uid`, `GUID`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

-- Device
DROP TABLE IF EXISTS `{prefix}_Device`;
CREATE TABLE `{prefix}_Device` (
  `Uid`		 	INT,
  `GUID`     	VARCHAR(64)		COLLATE utf8_general_ci,
  `LUID`     	VARCHAR(64) 	COLLATE utf8_general_ci,
  `Group`	 	VARCHAR(256) 	COLLATE utf8_general_ci,
  `Type`	 	VARCHAR(1) 		COLLATE utf8_general_ci,
  `SyncStat`	VARCHAR(3)		COLLATE utf8_general_ci,
  `XML`      	MEDIUMTEXT		COLLATE utf8_general_ci,
  PRIMARY KEY (`Uid`, `GUID`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

-- Attachments
DROP TABLE IF EXISTS `{prefix}_Attachments`;
CREATE TABLE `{prefix}_Attachments` (
  `Uid`		 	INT,
  `GUID`     	VARCHAR(64)		COLLATE utf8_general_ci,
  `LUID`     	VARCHAR(64) 	COLLATE utf8_general_ci,
  `Group`	 	VARCHAR(256) 	COLLATE utf8_general_ci,
  `Type`	 	VARCHAR(1) 		COLLATE utf8_general_ci,
  `SyncStat`	VARCHAR(3)		COLLATE utf8_general_ci,
  `XML`      	MEDIUMTEXT		COLLATE utf8_general_ci,
  PRIMARY KEY (`Uid`, `GUID`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

-- Contact
DROP TABLE IF EXISTS `{prefix}_Contact`;
CREATE TABLE `{prefix}_Contact` (
  `Uid`		 	INT,
  `GUID`     	VARCHAR(64)		COLLATE utf8_general_ci,
  `LUID`     	VARCHAR(64) 	COLLATE utf8_general_ci,
  `Group`	 	VARCHAR(256) 	COLLATE utf8_general_ci,
  `Type`	 	VARCHAR(1) 		COLLATE utf8_general_ci,
  `SyncStat`	VARCHAR(3)		COLLATE utf8_general_ci,
  `XML`      	MEDIUMTEXT		COLLATE utf8_general_ci,
  PRIMARY KEY (`Uid`, `GUID`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

-- Calendar
DROP TABLE IF EXISTS `{prefix}_Calendar`;
CREATE TABLE `{prefix}_Calendar` (
  `Uid`		 	INT,
  `GUID`     	VARCHAR(64)		COLLATE utf8_general_ci,
  `LUID`     	VARCHAR(64) 	COLLATE utf8_general_ci,
  `Group`	 	VARCHAR(256) 	COLLATE utf8_general_ci,
  `Type`	 	VARCHAR(1) 		COLLATE utf8_general_ci,
  `SyncStat`	VARCHAR(3)		COLLATE utf8_general_ci,
  `XML`      	MEDIUMTEXT		COLLATE utf8_general_ci,
  PRIMARY KEY (`Uid`, `GUID`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

-- Task
DROP TABLE IF EXISTS `{prefix}_Task`;
CREATE TABLE `{prefix}_Task` (
  `Uid`		 	INT,
  `GUID`     	VARCHAR(64)		COLLATE utf8_general_ci,
  `LUID`     	VARCHAR(64) 	COLLATE utf8_general_ci,
  `Group`	 	VARCHAR(256) 	COLLATE utf8_general_ci,
  `Type`	 	VARCHAR(1) 		COLLATE utf8_general_ci,
  `SyncStat`	VARCHAR(3)		COLLATE utf8_general_ci,
  `XML`      	MEDIUMTEXT		COLLATE utf8_general_ci,
  PRIMARY KEY (`Uid`, `GUID`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

-- Note
DROP TABLE IF EXISTS `{prefix}_Note`;
CREATE TABLE `{prefix}_Note` (
  `Uid`		 	INT,
  `GUID`     	VARCHAR(64)		COLLATE utf8_general_ci,
  `LUID`     	VARCHAR(64) 	COLLATE utf8_general_ci,
  `Group`	 	VARCHAR(256) 	COLLATE utf8_general_ci,
  `Type`	 	VARCHAR(1) 		COLLATE utf8_general_ci,
  `SyncStat`	VARCHAR(3)		COLLATE utf8_general_ci,
  `XML`      	MEDIUMTEXT		COLLATE utf8_general_ci,
  PRIMARY KEY (`Uid`, `GUID`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

-- EMail
DROP TABLE IF EXISTS `{prefix}_Mail`;
CREATE TABLE `{prefix}_Mail` (
  `Uid`		 	INT,
  `GUID`     	VARCHAR(64)		COLLATE utf8_general_ci,
  `LUID`     	VARCHAR(64) 	COLLATE utf8_general_ci,
  `Group`	 	VARCHAR(256) 	COLLATE utf8_general_ci,
  `Type`	 	VARCHAR(1) 		COLLATE utf8_general_ci,
  `SyncStat`	VARCHAR(3)		COLLATE utf8_general_ci,
  `XML`      	MEDIUMTEXT		COLLATE utf8_general_ci,
  PRIMARY KEY (`Uid`, `GUID`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

-- SMS
DROP TABLE IF EXISTS `{prefix}_SMS`;
CREATE TABLE `{prefix}_SMS` (
  `Uid`		 	INT,
  `GUID`     	VARCHAR(64)		COLLATE utf8_general_ci,
  `LUID`     	VARCHAR(64) 	COLLATE utf8_general_ci,
  `Group`	 	VARCHAR(256) 	COLLATE utf8_general_ci,
  `Type`	 	VARCHAR(1) 		COLLATE utf8_general_ci,
  `SyncStat`	VARCHAR(3)		COLLATE utf8_general_ci,
  `XML`      	MEDIUMTEXT		COLLATE utf8_general_ci,
  PRIMARY KEY (`Uid`, `GUID`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

-- DocLib
DROP TABLE IF EXISTS `{prefix}_DocLib`;
CREATE TABLE `{prefix}_DocLib` (
  `Uid`		 	INT,
  `GUID`     	VARCHAR(64)		COLLATE utf8_general_ci,
  `LUID`     	VARCHAR(64) 	COLLATE utf8_general_ci,
  `Group`	 	VARCHAR(256) 	COLLATE utf8_general_ci,
  `Type`	 	VARCHAR(1) 		COLLATE utf8_general_ci,
  `SyncStat`	VARCHAR(3)		COLLATE utf8_general_ci,
  `XML`      	MEDIUMTEXT		COLLATE utf8_general_ci,
  PRIMARY KEY (`Uid`, `GUID`)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;
