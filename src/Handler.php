<?php
declare(strict_types=1);

/*
 * 	MySQL data base handler class
 *
 *	@package	sync*gw
 *	@subpackage	mySQL handler
 *	@copyright	(c) 2008 - 2024 Florian Daeumling, Germany. All right reserved
 * 	@license 	LGPL-3.0-or-later
 */

namespace syncgw\interface\mysql;

use Exception;
use syncgw\lib\Config;
use syncgw\lib\DB;
use syncgw\lib\DataStore;
use syncgw\lib\ErrorHandler;
use syncgw\lib\Log;
use syncgw\lib\Msg;
use syncgw\lib\Server;
use syncgw\lib\User;
use syncgw\lib\Util;
use syncgw\lib\XML;
use syncgw\interface\DBintHandler;

class Handler implements DBintHandler {

	/**
	 * 	Data base handler
	 * 	@var \mysqli
	 */
	private static $_db = null;

	/**
	 * 	Table names
	 * 	@var array
	 */
	private static $_tab = [];

    /**
     * 	Singleton instance of object
     * 	@var Handler
     */
    private static $_obj = null;

    /**
	 *  Get class instance handler
	 *
	 *  @return - Class object
	 */
	public static function getInstance() {

		if (!self::$_obj) {

            self::$_obj = new self();

			// set log message codes 20101-20200
			$log = Log::getInstance();
			$log->setLogMsg([
					20101 => 'SQL Error: %s',
					20102 => 20101,
					20103 => 'User ID for user (%s) not set',
					20104 => 'Invalid XML data in record \'%s\' in %s data store for user (%s)',
			]);

			// save table names
			$cnf = Config::getInstance();
			$pre = $cnf->getVar(Config::DB_PREF);
			foreach (Util::HID(Util::HID_TAB, DataStore::ALL, true) as $k => $v)
				self::$_tab[$k] = '`'.$pre.'_'.$v.'`';

			// check data base access parameter
			$conf = [];
			foreach ([ Config::DB_HOST, Config::DB_PORT, Config::DB_USR,
					   Config::DB_UPW, Config::DB_NAME, Config::DB_PREF ] as $k) {

				if (!($conf[$k] = $cnf->getVar($k)))
					return null;
			}

			// connect to data base
			Msg::InfoMsg('Connecting to data base "'.$conf[Config::DB_NAME].'" on "'.
						$conf[Config::DB_HOST].':'.$conf[Config::DB_PORT].'" with user "'.
						$conf[Config::DB_USR].'" and password "'.$conf[Config::DB_UPW].'"');
			self::$_db = new \mysqli($conf[Config::DB_HOST], $conf[Config::DB_USR], $conf[Config::DB_UPW],
										   $conf[Config::DB_NAME], $conf[Config::DB_PORT]);
			if ($msg = \mysqli_connect_error()) {

				$log->logMsg(Log::ERR, 20101, $msg);
				return null;
			}

			// register shutdown function
			Server::getInstance()->regShutdown(__CLASS__);
		}

		return self::$_obj;
	}

 	/**
	 * 	Shutdown function
	 */
	public function delInstance(): void {

		self::$_obj = null;
	}

    /**
	 * 	Collect information about class
	 *
	 * 	@param 	- Object to store information
	 */
	public function getInfo(XML &$xml): void {

    	$xml->addVar('Name', 'Improved MySQL interface handler');

		$xml->addVar('Opt', 'Status');
		$cnf = Config::getInstance();
		if (($v = $cnf->getVar(Config::DATABASE)) == 'mysql')
			$xml->addVar('Stat', 'Enabled');
		elseif ($v == 'file')
			$xml->addVar('Stat', 'Disabled');
		else
			$xml->addVar('Stat', 'Sustentative');
	}

	/**
	 * 	Perform query on internal data base
	 *
	 * 	@param	- Handler ID
	 * 	@param	- Query command:<fieldset>
	 * 			  DataStore::ADD 	  Add record                             $parm= XML object<br>
	 * 			  DataStore::UPD 	  Update record                          $parm= XML object<br>
	 * 			  DataStore::DEL	  Delete record or group (inc. sub-recs) $parm= GUID<br>
	 * 			  DataStore::RLID     Read single record                     $parm= LUID<br>
	 * 			  DataStore::RGID     Read single record       	             $parm= GUID<br>
	 * 			  DataStore::GRPS     Read all group records                 $parm= None<br>
	 * 			  DataStore::RIDS     Read all records in group              $parm= Group ID or '' for record in base group<br>
	 * 			  DataStore::RNOK     Read recs with SyncStat != STAT_OK     $parm= Group ID
	 * 	@return	- According  to input parameter<fieldset>
	 * 			  DataStore::ADD 	  New record ID or false on error<br>
	 * 			  DataStore::UPD 	  true=Ok; false=Error<br>
	 * 			  DataStore::DEL	  true=Ok; false=Error<br>
	 * 			  DataStore::RLID     XML object; false=Error<br>
	 * 			  DataStore::RGID	  XML object; false=Error<br>
	 * 			  DataStore::RIDS     [ "GUID" => Typ of record ]<br>
	 * 			  DataStore::GRPS	  [ "GUID" => Typ of record ]<br>
	 * 			  DataStore::RNOK     [ "GUID" => Typ of record ]
	 */
	public function Query(int $hid, int $cmd, $parm = null) {

		$log = Log::getInstance();

		if (!self::$_db|| ($hid & DataStore::EXT))
			return ($cmd & (DataStore::RIDS|DataStore::RNOK|DataStore::GRPS)) ? [] : false;

		if (($hid & DataStore::SYSTEM))
			$uid = '0';
		else {

			// get user ID
			$usr = User::getInstance();
			if (!($uid = $usr->getVar('LUID'))) {

				if (Config::getInstance()->getVar(Config::DBG_SCRIPT))
					$uid = '11';
				else {

					$log->logMsg(Log::ERR, 20103, $usr->getVar('GUID'));
					return $cmd & (DataStore::RIDS|DataStore::RNOK|DataStore::GRPS) ? [] : false;
				}
			}
		}

		// replace parameter
		switch ($cmd) {
		case DataStore::ADD:
			foreach ([
				'Uid' 		=> $uid,
				'GUID'		=> $parm->getVar('GUID'),
				'LUID'		=> $parm->getVar('LUID'),
				'Group'		=> $parm->getVar('Group'),
				'Type'		=> $parm->getVar('Type'),
				'SyncStat'	=> $parm->getVar('SyncStat'),
				'XML'		=> $parm->saveXML(true), ] as $key => $var)
				if (!is_string($var)) {

					Msg::ErrMsg($parm, 'ADD: Variable "'.$key.'" in "'.self::$_tab[$hid].
												'" has value "'.$var.'"');
					$parm->addVar($key, '');
				}
			$qry = 'INSERT '.self::$_tab[$hid].
			       ' SET '.
			       '   `Uid` = '.$uid.','.
				   '   `GUID` = "'.self::$_db->real_escape_string($out = $parm->getVar('GUID')).'",'.
				   '   `LUID` = "'.self::$_db->real_escape_string($parm->getVar('LUID')).'",'.
				   '   `Group` = "'.self::$_db->real_escape_string($parm->getVar('Group')).'",'.
				   '   `Type` = "'.self::$_db->real_escape_string($parm->getVar('Type')).'",'.
				   '   `SyncStat` = "'.self::$_db->real_escape_string($parm->getVar('SyncStat')).'",'.
				   '   `XML` = "'.self::$_db->real_escape_string($parm->saveXML(true)).'"';
			return self::_query($hid, $cmd, $qry) ? $out : false;

		case DataStore::UPD:
			foreach ([
				'Uid' 		=> $uid,
				'GUID'		=> $parm->getVar('GUID'),
				'LUID'		=> $parm->getVar('LUID'),
				'Group'		=> $parm->getVar('Group'),
				'Type'		=> $parm->getVar('Type'),
				'SyncStat'	=> $parm->getVar('SyncStat'),
				'XML'		=> $parm->saveXML(true), ] as $key => $var)
				if (!is_string($var)) {

					$parm->setTop();
					Msg::ErrMsg($parm, 'UPD: Variable "'.$key.'" in "'.
													self::$_tab[$hid].'" has value "'.$var.'"');
					$parm->updVar($key, '');
				}
			$qry = 'UPDATE '.self::$_tab[$hid].
				   ' SET'.
				   '   `LUID` = "'.self::$_db->real_escape_string($parm->getVar('LUID')).'",'.
				   '   `Type` = "'.self::$_db->real_escape_string($parm->getVar('Type')).'",'.
				   '   `SyncStat` = "'.self::$_db->real_escape_string($parm->getVar('SyncStat')).'",'.
				   '   `Group` = "'.self::$_db->real_escape_string($parm->getVar('Group')).'",'.
				   '   `XML` = "'.self::$_db->real_escape_string($parm->saveXML(true)).'"'.
				   ' WHERE `Uid` = "'.$uid.'"'.
				   ' AND `GUID` = "'.self::$_db->real_escape_string($parm->getVar('GUID')).'"';
			return self::_query($hid, $cmd, $qry);

		case DataStore::DEL:
			foreach ([
				'Uid' 	=> $uid,
				'GUID'	=> $parm, ] as $key => $var)
				if (!is_string($var)) {

					Msg::ErrMsg($parm, 'DEL: Variable "'.$key.'" in "'.
												self::$_tab[$hid].'" has value "'.$var.'"');
					$parm = strval($parm);
				}
			$qry = 'DELETE FROM '.self::$_tab[$hid].
			       '  WHERE `Uid` = "'.$uid.'"'.
				   '  AND `GUID` = "'.self::$_db->real_escape_string($parm).'"';
			return self::_query($hid, $cmd, $qry);

		case DataStore::RGID:
			foreach ([
				'Uid' 		=> $uid,
				'GUID'		=> $parm, ] as $key => $var)
				if (!is_string($var)) {

					Msg::ErrMsg($parm, 'RGID: Variable "'.$key.'" in "'.
											self::$_tab[$hid].'" has value "'.$var.'"');
					$parm = strval($parm);
				}
			$qry = 'SELECT `XML` FROM '.self::$_tab[$hid].
				   '  WHERE `Uid` = "'.$uid.'"'.
			   	   '  AND `GUID` = "'.self::$_db->real_escape_string($parm).'"';
			if (!($str = self::_query($hid, $cmd, $qry)))
				return false;
			break;

		case DataStore::RLID:
			foreach ([
				'Uid' 		=> $uid,
				'GUID'		=> $parm, ] as $key => $var)
				if (!is_string($var)) {

					Msg::ErrMsg($parm, 'RLID: Variable "'.$key.'" in "'.self::$_tab[$hid].'" has value "'.$var.'"');
					$parm = strval($parm);
				}
			$qry = 'SELECT `XML` FROM '.self::$_tab[$hid].
				   '  WHERE `Uid` = "'.$uid.'"'.
				   '  AND `LUID` = "'.self::$_db->real_escape_string($parm).'"';
			if (!($str = self::_query($hid, $cmd, $qry)))
				return false;
			break;

		case DataStore::GRPS:
			$qry = 'SELECT `GUID`, `Type` FROM '.self::$_tab[$hid].
			 	   '  WHERE `Uid` = "'.$uid.'"'.
				   '  AND `Type` = "'.DataStore::TYP_GROUP.'"';
			$out = [];
			$gid = 0;
			$id  = true;
			foreach (self::_query($hid, $cmd, $qry) as $k) {
				if (!$id) {

					$out[$gid] = $k;
					$id  = true;
				} else {

					$gid = $k;
					$id  = false;
				}
			}
			return $out;

		case DataStore::RIDS:
			foreach ([
				'Uid' 	=> $uid,
				'Group'	=> $parm, ] as $key => $var)
				if (!is_string($var)) {

					Msg::ErrMsg($parm, 'GRPS: Variable "'.$key.'" in "'.self::$_tab[$hid].'" has value "'.$var.'"');
					$parm = strval($parm);
				}
			$qry = 'SELECT `GUID`, `Type` FROM '.self::$_tab[$hid].
			 	   '  WHERE `Uid` = "'.$uid.'"'.
				   '  AND `Group` = "'.self::$_db->real_escape_string($parm).'"';
			$out = [];
			$gid = 0;
			$id  = true;
			foreach (self::_query($hid, $cmd, $qry) as $k) {

				if (!$id) {

					$out[$gid] = $k;
					$id  = true;
				} else {

					$gid = $k;
					$id  = false;
				}
			}
			return $out;

		case DataStore::RNOK:
			foreach ([

				'Uid' 	=> $uid,
				'Group'	=> $parm ] as $key => $var)
				if (!is_string($var)) {

					Msg::ErrMsg($parm, 'RNOK: Variable "'.$key.'" in "'.self::$_tab[$hid].'" has value "'.$var.'"');
					$parm = strval($parm);
				}
			$qry = 'SELECT GUID, Type FROM '.self::$_tab[$hid].
				   '  WHERE `Uid` = "'.$uid.'"'.
				   '  AND `SyncStat` <> "'.DataStore::STAT_OK.'"'.
				   '  AND `Group` = "'.self::$_db->real_escape_string($parm).'"';
			$out = [];
			$gid = '';
			$id  = true;
			foreach (self::_query($hid, $cmd, $qry) as $k) {

				if (!$id) {

					$out[$gid] = $k;
					$id  = true;
				} else {

					$gid = $k;
					$id  = false;
				}
			}
			return $out;

		default:
		    return false;
		}

		$xml = new XML();
		if (!$xml->loadXML($str)) {

			$id = [];
			// extract <GUID> from record to get reference record number for error message
			preg_match('#(?<=\<GUID\>).*(?=\</GUID\>)#', '', $id);
			ErrorHandler::getInstance()->Raise(20103, isset($id[0]) ? $id[0] : '', Util::HID(Util::HID_ENAME, $hid), $uid);
			return false;
		}

		return $xml;
	}

	/**
	 * 	Excute raw SQL query on internal data base
	 *
	 * 	@param	- SQL query string
	 * 	@return	- Result string or []; null on error
	 */
	public function SQL(string $query) {

		return self::_query(0, 0, $query);
	}

	/**
	 * 	Execute query
	 *
	 * 	@param	- Handler ID
	 * 	@param	- Query command
	 * 	@param	- Query string
	 * 	@return	- String or []; null=Error
	 */
	private function _query(int $hid, int $cmd, string $qry) {

		$msg = Log::getInstance();

		$dmsg = ($cmd ? DB::OPS[$cmd] : 'SQL').': '.preg_replace('/(?<=<syncgw>).*(?=<\/syncgw>)/', 'XML-Data', $qry);

		// lock table
		if ($cmd & (DataStore::ADD|DataStore::UPD|DataStore::DEL))
			self::_query($hid, 0, 'LOCK TABLES '.self::$_tab[$hid].' WRITE;');

		// return value
		$out = null;
		$cnf = Config::getInstance();
		$cnt = $cnf->getVar(Config::DB_RETRY);

		do {
			$smsg = '';
			try {
				$obj = self::$_db->query($qry);
			}
			catch (Exception  $e) {
				// Uncaught mysqli_sql_exception: Duplicate entry
				if ($e->getCode() == 1062) {

					$t = explode('\'', $e->getMessage());
					$obj = $t[1];
				} else
					$smsg = '['.$e->getCode().'] '.$e->getMessage();
			}
			if (!isset($obj) || $obj === false || $smsg) {

				if (!$smsg)
					$smsg = self::$_db->connect_error ?
						   '['.self::$_db->connect_errno.'] '.self::$_db->connect_error :
						   '['.self::$_db->errno.'] ('.self::$_db->error.'), SQLSTATE: '.self::$_db->sqlstate;

				// [1146] (42S02): Table 'xxx' doesn't exist -> table is not locked
				if (self::$_db->errno == 1146 && !$cmd)
					return null;

				// [2006] MySQL server has gone away
				if (self::$_db->errno == 2006) {

					if ($cnt--) {

						Util::Sleep(300);
						Log::getInstance()->logMsg(Log::DEBUG, 20103, $smsg);
					}
				} else {

					$cnt = 0;
					Log::getInstance()->logMsg(Log::ERR, 20102, $smsg);
					foreach (ErrorHandler::Stack() as $rec)
						Log::getInstance()->logMsg(Log::DEBUG, 11601, $rec);

					$out = $cmd & (DataStore::RIDS|DataStore::RNOK|DataStore::GRPS) ? [] : null;
				}
			} else {

				$cnt = 0;

				// do not save return data for LOCK and UNLOCK directives
				if (strncmp($qry, 'LOCK', 4) && strncmp($qry, 'UNLO', 4)) {

					Msg::InfoMsg($dmsg);
					if (is_object($obj)) {

						$wrk = [];
						if (!$cmd) {

							while ($row = $obj->fetch_assoc())
								$wrk[] = $row;
						} else {

							while ($row = $obj->fetch_row())
								$wrk = array_merge($wrk, $row);
						}
						$obj->free();
						$out = [];
						foreach ($wrk as $rec) {

							// did we receive array as return value?
							if (is_array($rec)) {

								$out[] = $rec;
								continue;
							}
							if (substr($rec, 0, 1) == '<')
								$out = $rec;
							else
								$out[] = $rec;
						}
					} else
						$out = $obj;
				}
			}
		} while ($cnt);

		if ($cmd & (DataStore::RGID|DataStore::RLID) && is_array($out) && count($out))
		    $out = strval($out[0]);

		// unlock table?
		if ($cmd & (DataStore::ADD|DataStore::UPD|DataStore::DEL))
			self::_query($hid, 0, 'UNLOCK TABLES;');

		// check for empty records
		if ($cmd & (DataStore::RGID|DataStore::RLID) && is_bool($out))
			return false;

		return $out;
	}

}
