<?php
declare(strict_types=1);

/*
 * 	Administration interface handler class
 *
 *	@package	sync*gw
 *	@subpackage	mySQL handler
 *	@copyright	(c) 2008 - 2025 Florian Daeumling, Germany. All right reserved
 * 	@license 	LGPL-3.0-or-later
 */

namespace syncgw\interface\mysql;

use syncgw\interface\DBAdmin;
use syncgw\lib\Config;
use syncgw\lib\DataStore;
use syncgw\gui\guiHandler;

class Admin implements DBAdmin {

    /**
     * 	Singleton instance of object
     * 	@var Admin
     */
    static private $_obj = null;

    /**
	 *  Get class instance handler
	 *
	 *  @return - Class object
	 */
	public static function getInstance(): Admin {

	   	if (!self::$_obj)
            self::$_obj = new self();

		return self::$_obj;
	}

 	/**
	 * 	Show/get installation parameter
	 */
	public function getParms(): void {

		$gui = guiHandler::getInstance();
		$cnf = Config::getInstance();

		if(!($c = $gui->getVar('MySQLHost')))
			$c = $cnf->getVar(Config::DB_HOST);
		$gui->putQBox('MySQL server name',
					'<input name="MySQLHost" type="text" size="40" maxlength="250" value="'.$c.'" />',
					'MySQL server name (default: "localhost").', false);
		if(!($c = $gui->getVar('MySQLPort')))
			$c = $cnf->getVar(Config::DB_PORT);
		$gui->putQBox('MySQL port address',
					'<input name="MySQLPort" type="text" size="5" maxlength="6" value="'.$c.'" />',
					'MySQL server port (default: 3306).', false);
		if(!($c = $gui->getVar('MySQLName')))
			$c = $cnf->getVar(Config::DB_NAME);
		$gui->putQBox('MySQL data base name',
					'<input name="MySQLName" type="text" size="30" maxlength="64" value="'.$c.'" />',
					'Name of MySQL data base to store tables. The tables will be created automatically.', false);
		if(!($c = $gui->getVar('MySQLUsr')))
			$c = $cnf->getVar(Config::DB_USR);
		$gui->putQBox('MySQL data base user name',
					'<input name="MySQLUsr" type="text" size="20" maxlength="40" value="'.$c.'" />',
					'User name to access MySQL data base.', false);
		if(!($c = $gui->getVar('MySQLPwd')))
			$c = $cnf->getVar(Config::DB_UPW);
		$gui->putQBox('MySQL data base user password',
					'<input name="MySQLPwd" type="password" size="20" maxlength="40" value="'.$c.'" />',
					'Password for MySQL data base user.', false);
		if(!($c = $gui->getVar('MySQLPref')))
			$c = $cnf->getVar(Config::DB_PREF);
		$gui->putQBox('MySQL <strong>sync&bull;gw</strong> data base table name prefix',
					'<input name="MySQLPref" type="text" size="20" maxlength="40" value="'.$c.'" />',
					'Table name prefix for <strong>sync&bull;gw</strong> data base tables '.
					'(to avaoid duplicate table names in data base).', false);
	}

	/**
	 * 	Connect to handler
	 *
	 * 	@return - true=Ok; false=Error
	 */
	public function Connect(): bool {

		$gui = guiHandler::getInstance();
		$cnf = Config::getInstance();

		// connection established?
		if ($cnf->getVar(Config::DATABASE))
			return true;

		// swap variables
		$cnf->updVar(Config::DB_NAME, $gui->getVar('MySQLName'));
		if (!$cnf->getVar(Config::DB_NAME)) {

			$gui->clearAjax();
			$gui->putMsg('Missing MySQL data base name', Config::CSS_ERR);
			return false;
		}
		$cnf->updVar(Config::DB_HOST, $gui->getVar('MySQLHost'));
		if (!$cnf->getVar(Config::DB_HOST)) {

			$gui->clearAjax();
			$gui->putMsg('Missing MySQL host name', Config::CSS_ERR);
			return false;
		}
		$cnf->updVar(Config::DB_PORT, $gui->getVar('MySQLPort'));
		if (!$cnf->getVar(Config::DB_PORT)) {

			$gui->clearAjax();
			$gui->putMsg('Missing MySQL port name', Config::CSS_ERR);
			return false;
		}
		$cnf->updVar(Config::DB_USR, $gui->getVar('MySQLUsr'));
		if (!$cnf->getVar(Config::DB_USR)) {

			$gui->clearAjax();
			$gui->putMsg('Missing MySQL data base user name', Config::CSS_ERR);
			return false;
		}
		$cnf->updVar(Config::DB_UPW, $gui->getVar('MySQLPwd'));
		if (!$cnf->getVar(Config::DB_UPW)) {

			$gui->clearAjax();
			$gui->putMsg('Missing MySQL data base user password', Config::CSS_ERR);
			return false;
		}
		$cnf->updVar(Config::DB_PREF, $gui->getVar('MySQLPref'));

		// create tables
		return self::mkTable();
	}

	/**
	 * 	Disconnect from handler
	 *
	 * 	@return - true=Ok; false=Error
	 */
	public function DisConnect(): bool {

		return self::delTable();
	}

	/**
	 * 	Return list of supported data store handler
	 *
	 * 	@return - Bit map of supported data store handler
	 */
	public function SupportedHandlers(): int {

		return DataStore::DATASTORES&~DataStore::MAIL;
	}

	/**
	 * 	Create data base tables
	 *
	 * 	@param	- Optional SQL commands
	 * 	@return	- true=Ok; false=Error
	 */
	public function mkTable(?array $cmds = null): bool {

		$gui = guiHandler::getInstance();
		$cnf = Config::getInstance();

		// allocate MySQL handler
		$db = Handler::getInstance();

		if (!$cmds)
			$cmds = self::loadSQL(__DIR__.'/../assets/tables.sql');

		$gui->clearAjax();

		// perform installation
		$pref = $cnf->getVar(Config::DB_PREF);
		foreach ($cmds as $cmd) {

			$cmd = str_replace([ "\n", '{prefix}' ], [ '', $pref ], $cmd);
			if (!strlen(trim($cmd)))
				continue;

			if (!$db->SQL($cmd)) {

				$gui->putMsg(sprintf('Error executing SQL command: "%s"', $cmd), Config::CSS_ERR);
				return false;
			}
		}
		$gui->putMsg('<strong>sync&bull;gw</strong> MySQL tables created');

		return true;
	}

	/**
	 * 	Delete data base table
	 *
	 * 	@param	- Optional SQL commands
	 * 	@return	- true=Ok; false=Error
	 */
	public function delTable(?array $cmds = null): bool {

		$gui = guiHandler::getInstance();
		$cnf = Config::getInstance();

		// allocate MySQL handler
		$db = Handler::getInstance();

		if (!$cmds)
			$cmds = self::loadSQL(__DIR__.'/../assets/tables.sql');

		$gui->clearAjax();

		// perform deinstallation
		$pref = $cnf->getVar(Config::DB_PREF);
		foreach ($cmds as $cmd) {

			if (!strlen(trim($cmd)) || stripos($cmd, 'DROP') === false)
				continue;

			$cmd = str_replace('{prefix}', $pref, $cmd);
			if (!$db->SQL($cmd)) {

				$gui->putMsg(sprintf('Error executing SQL command: "%s"', $cmd), Config::CSS_ERR);
				return false;
			}
		}
		$gui->putMsg('<strong>sync&bull;gw</strong> MySQL tables deleted');

		return true;
	}

	/**
	 * 	Load SQL statements
	 *
	 * 	@param	- File name to load
	 * 	@return	- Command list or emtpy []
	 */
	public function loadSQL(string $file): array {

		$gui = guiHandler::getInstance();

		$path = realpath($file);
		if (!$path || !($cmds = @file_get_contents($path))) {

			$gui->putMsg(sprintf('Error loading MySQL tables from \'%s\'', $file), Config::CSS_ERR);
			return [];
		}

		$recs = explode("\n", $cmds);
		$wrk = '';
		foreach ($recs as $rec) {

			$rec = trim($rec);
			// strip comment lines
			if (!strlen($rec) || substr($rec, 0, 2) == '--')
				continue;
			$wrk .= $rec;
		}

		return explode(';', substr($wrk, 0, -1));
	}

}
