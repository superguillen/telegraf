package oracledb

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/inputs"
	go_ora "github.com/sijms/go-ora/v2"
)

type OracleDB struct {
	Server                                    string        `toml:"server"`
	User                                      config.Secret `toml:"user"`
	Password                                  config.Secret `toml:"password"`
	Base64Password                            string        `toml:"base64_password"`
	GatherDatabaseInstanceDetails             bool          `toml:"gather_database_instance_details"`
	GatherDatabaseInstanceSysmetric           bool          `toml:"gather_database_instance_sysmetric"`
	GatherDatabaseInstanceWaitStats           bool          `toml:"gather_database_instance_wait_stats"`
	GatherDatabaseInstanceWaitClassStats      bool          `toml:"gather_database_instance_wait_class_stats"`
	GatherDatabaseInstanceUserSessions        bool          `toml:"gather_database_instance_user_sessions"`
	GatherDatabaseInstanceUserSessionsDetails bool          `toml:"gather_database_instance_user_sessions_details"`
	GatherDatabaseInstanceTablespaces         bool          `toml:"gather_database_instance_tablespaces"`
	GatherDatabaseInstanceSGA                 bool          `toml:"gather_database_instance_sga"`
	GatherDatabaseInstanceSQLStats            bool          `toml:"gather_database_instance_sqlstats"`
	GatherDatabaseInstanceSysStat             bool          `toml:"gather_database_instance_sysstat"`
	GatherDatabaseInstanceSystemEvent         bool          `toml:"gather_database_instance_system_event"`
	GatherDatabaseInstanceSysTimeModel        bool          `toml:"gather_database_instance_sys_time_model"`

	Log telegraf.Logger `toml:"-"`
}

type OracleInstance struct {
	host                    string
	instance_name           string
	container_name          string
	pluglable_database_name string
	instance_number         int
}

const sampleConfig = `
  ## If no servers are specified, then localhost is used as the host.
  # server = "localhost:1521/XCD"
  # user = "user"
  #For password in base64 format 
  # base64_password = "password"
  #For plaint text or telegraf SecretStore
  # password = "password"
  # gather_database_instance_details = true
  # gather_database_instance_sysmetric = true
  # gather_database_instance_wait_stats = true
  # gather_database_instance_wait_class_stats = true
  # gather_database_instance_user_sessions = true
  # gather_database_instance_user_sessions_details = true
  # gather_database_instance_tablespaces = true
  # gather_database_instance_sga = true
  # gather_database_instance_sysstat = true
  # gather_database_instance_system_event = true
  # gather_database_instance_sys_time_model = true
  ########################################
  # Note: gather_database_instance_sqlstats = true only for databases with cursor_sharing=force (higth cardinality whit other that force)
  # gather_database_instance_sqlstats = true
`

const (
	defaultServer                                    = "localhost:1521/XSC"
	defaultGatherDatabaseInstanceDetails             = true
	defaultGatherDatabaseInstanceSysmetric           = true
	defaultGatherDatabaseInstanceWaitStats           = true
	defaultGatherDatabaseInstanceWaitClassStats      = true
	defaultGatherDatabaseInstanceUserSessions        = true
	defaultGatherDatabaseInstanceUserSessionsDetails = true
	defaultGatherDatabaseInstanceTablespaces         = true
	defaultGatherDatabaseInstanceSGA                 = true
	defaultGatherDatabaseInstanceSQLStats            = false
	defaultGatherDatabaseInstanceSysStat             = true
	defaultGatherDatabaseInstanceSystemEvent         = true
	defaultGatherDatabaseInstanceSysTimeModel        = true
)

func (m *OracleDB) Init() error {
	return nil
}

func (m *OracleDB) SampleConfig() string {
	return sampleConfig
}

func (m *OracleDB) Description() string {
	return "Read metrics from Oracle Database"
}

func (m *OracleDB) Gather(acc telegraf.Accumulator) error {
	var password string
	var username string

	if !m.User.Empty() {
		username_secret, err := m.User.Get()
		if err != nil {
			return err
		} else {
			username = username_secret.String()
		}
		defer username_secret.Destroy()
	}

	if !m.Password.Empty() {
		password_secret, err := m.Password.Get()
		if err != nil {
			return err
		} else {
			password = password_secret.String()
		}
		defer password_secret.Destroy()
	} else {
		password_b64, err := base64.StdEncoding.DecodeString(m.Base64Password)
		if err != nil {
			return err
		} else {
			password = string(password_b64)
		}
	}

	userurl := url.UserPassword(username, password).String()

	//m.Log.Infof("oracle://%s@%s", userurl, m.Server)

	dbUrl := fmt.Sprintf("oracle://%s@%s", userurl, m.Server)

	db, err := m.getConnection(dbUrl)
	if err != nil {
		return err
	}

	defer db.Close()

	var instanceInfo OracleInstance
	acc.AddError(m.gatherServer(&instanceInfo, db, acc))

	return nil
}

// metric queries
const (
	databaseInstanceInfoQuery = `
	SELECT INSTANCE_NAME
	      ,INSTANCE_NUMBER
	      ,HOST_NAME
		  ,SYS_CONTEXT('USERENV', 'CON_NAME') CONTAINER_NAME
		   FROM SYS.V_$INSTANCE
	`
	databaseInstanceDetailsQuery = `
	SELECT INSTANCE_NAME
	      ,INSTANCE_NUMBER
	      ,HOST_NAME
	      ,STATUS
		  ,DATABASE_STATUS
		  ,INSTANCE_ROLE
		  ,VERSION
		  ,EDITION
		  ,DATABASE_TYPE
		  ,(sysdate-startup_time)*24*60*60 STARTUP_SECS
	FROM SYS.V_$INSTANCE
	`
	databaseInstanceSysmetricQuery = `
	SELECT REPLACE(METRIC_NAME,' ','_') METRIC_NAME
	      ,VALUE METRIC_VALUE
		  ,REPLACE(METRIC_UNIT,' ','_') METRIC_UNIT
	FROM V$SYSMETRIC
	WHERE GROUP_ID=2
	`
	databaseInstanceWaitStatsQuery = `
	SELECT REPLACE(N.WAIT_CLASS,' ','_') WAIT_CLASS
		,REPLACE(N.NAME,' ','_') 	WAIT_EVENT
		,M.WAIT_COUNT 				WAIT_COUNT
		,ROUND(10*M.TIME_WAITED/NULLIF(M.WAIT_COUNT,0),3) LATENCY_MS
	FROM V$EVENTMETRIC M,
	V$EVENT_NAME N
	WHERE M.EVENT_ID=N.EVENT_ID
	AND N.WAIT_CLASS <> 'Idle' AND M.WAIT_COUNT > 0 ORDER BY 1
	`
	databaseInstanceWaitClassStatsQuery = `
	SELECT REPLACE(N.WAIT_CLASS,' ','_') WAIT_CLASS
	      ,ROUND(M.TIME_WAITED/M.INTSIZE_CSEC,3) WAIT_VALUE
	FROM   V$WAITCLASSMETRIC  M, V$SYSTEM_WAIT_CLASS N
	WHERE M.WAIT_CLASS_ID=N.WAIT_CLASS_ID AND N.WAIT_CLASS != 'Idle'
	UNION
	SELECT  'CPU' WAIT_CLASS
	       ,ROUND(VALUE/100,3) WAIT_VALUE
	FROM V$SYSMETRIC WHERE METRIC_NAME='CPU Usage Per Sec' AND GROUP_ID=2
	UNION 
	SELECT 'CPU_OS' WAIT_CLASS
	      ,ROUND((PRCNT.BUSY*PARAMETER.CPU_COUNT)/100,3) - AAS.CPU WAIT_VALUE
	FROM
	( SELECT VALUE BUSY  FROM V$SYSMETRIC  WHERE METRIC_NAME='Host CPU Utilization (%)' AND GROUP_ID=2 ) PRCNT,
	( SELECT VALUE CPU_COUNT FROM V$PARAMETER WHERE NAME='cpu_count' )  PARAMETER,
	( SELECT  'CPU', ROUND(VALUE/100,3) CPU FROM V$SYSMETRIC WHERE METRIC_NAME='CPU Usage Per Sec' AND GROUP_ID=2) AAS
	`
	databaseInstanceUserSessionsDetailsQuery = `
	SELECT USERNAME
		,STATUS
		,MACHINE
		,REPLACE(EVENT,' ','_') EVENT
		,NVL(SQL_ID,'None') SQL_ID
		,NVL2(LOCKWAIT,'WITH_LOCKWAIT','None') LOCKWAIT
		,DECODE(COMMAND,
			1,'CREATE_TABLE',2,'INSERT',
			3,'SELECT',6,'UPDATE',
			7,'DELETE',9,'CREATE_INDEX',
			10,'DROP_INDEX',11,'ALTER_INDEX',
			12,'DROP_TABLE',13,'CREATE_SEQUENCE',
			14,'ALTER_SEQUENCE',15,'ALTER_TABLE',
			16,'DROP_SEQUENCE',17,'GRANT',
			19,'CREATE_SYNONYM',20,'DROP_SYNONYM',
			21,'CREATE_VIEW',22,'DROP_VIEW',
			23,'VALIDATE_INDEX',24,'CREATE_PROCEDURE',
			25,'ALTER_PROCEDURE',26,'LOCK_TABLE',
			42,'ALTER_SESSION',44,'COMMIT',
			45,'ROLLBACK',46,'SAVEPOINT',
			47,'PL/SQL_EXEC',48,'SET_TRANSACTION',
			60,'ALTER_TRIGGER',62,'ANALYZE_TABLE',
			63,'ANALYZE_INDEX',71,'CREATE_SNAPSHOT_LOG',
			72,'ALTER_SNAPSHOT_LOG',73,'DROP_SNAPSHOT_LOG',
			74,'CREATE_SNAPSHOT',75,'ALTER_SNAPSHOT',
			76,'DROP_SNAPSHOT',85,'TRUNCATE_TABLE',
			0,'NO_COMMAND','? : '||COMMAND) COMMAND
		,MIN(LAST_CALL_ET) LAST_CALL_ET_MIN
		,MAX(LAST_CALL_ET) LAST_CALL_ET_MAX
		,AVG(LAST_CALL_ET) LAST_CALL_ET_AVG
		,MIN(WAIT_TIME) WAIT_TIME_MIN
		,MAX(WAIT_TIME) WAIT_TIME_MAX
		,AVG(WAIT_TIME) WAIT_TIME_AVG
		,COUNT(*) COUNT
	FROM V$SESSION
	WHERE USERNAME IS  NOT NULL
	GROUP BY USERNAME,STATUS,MACHINE,EVENT,SQL_ID,LOCKWAIT,COMMAND
	`
	databaseInstanceUserSessionsQuery = `
	SELECT USERNAME
		,STATUS
		,MIN(LAST_CALL_ET) LAST_CALL_ET_MIN
		,MAX(LAST_CALL_ET) LAST_CALL_ET_MAX
		,AVG(LAST_CALL_ET) LAST_CALL_ET_AVG
		,COUNT(*) SESSION_COUNT
	FROM V$SESSION
	WHERE USERNAME IS  NOT NULL
	GROUP BY USERNAME,STATUS,COMMAND
	`

	databaseInstanceTablespacesQuery = `
	SELECT TABLESPACE_NAME TBS_NAME
		,TOTAL_SPACE TOTAL_SPACE_MB
		,FREE_SPACE FREE_SPACE_MB
		,PERC_USED PERCENT_USED
		,PERCEXTEND_USED PERCENT_USED_AUTOEXT
		,MAX_SIZE_MB MAX_SIZE_MB
		,FREE_SPACE_EXTEND FREE_SPACE_AUTOEXTEND_MB
	FROM (
	SELECT T1.TABLESPACE_NAME,
		ROUND(USED_SPACE/1024/1024) TOTAL_SPACE,
		ROUND(NVL(LIB,0)/1024/1024) FREE_SPACE,
		ROUND(100*(USED_SPACE-NVL(LIB,0))/USED_SPACE,1) PERC_USED,
		ROUND(100*(USED_SPACE-NVL(LIB,0))/SMAX_BYTES,1) PERCEXTEND_USED,
		ROUND(NVL(SMAX_BYTES,0)/1024/1024) MAX_SIZE_MB,
		ROUND(NVL(SMAX_BYTES-(USED_SPACE-NVL(LIB,0)),0)/1024/1024) FREE_SPACE_EXTEND,
		NB_EXT NB_EXT
	FROM (SELECT TABLESPACE_NAME,SUM(BYTES) USED_SPACE FROM DBA_DATA_FILES I
	GROUP BY TABLESPACE_NAME) T1,
	(SELECT TABLESPACE_NAME,
			SUM(BYTES) LIB,
			MAX(BYTES) MAX_NB ,
			COUNT(BYTES) NB_EXT
	FROM DBA_FREE_SPACE
	GROUP BY TABLESPACE_NAME) T2,
	(SELECT TABLESPACE_NAME,SUM(MAX_BYTES) SMAX_BYTES
	FROM (SELECT TABLESPACE_NAME, CASE WHEN AUTOEXTENSIBLE = 'YES' THEN GREATEST(BYTES,MAXBYTES)
			ELSE BYTES END MAX_BYTES
			FROM DBA_DATA_FILES I)
	GROUP BY TABLESPACE_NAME ) T3
	WHERE T1.TABLESPACE_NAME=T2.TABLESPACE_NAME(+)
	AND T1.TABLESPACE_NAME=T3.TABLESPACE_NAME(+)
	)
	`

	databaseInstanceSGAQuery = `
	SELECT ROUND(SGA_TOTAL_MB,2) SGA_TOTAL_MB
		  ,ROUND(SGA_FREE_MB,2)  SGA_FREE_MB
		  ,ROUND(SGA_USED_MB,2)  SGA_USED_MB
		  ,ROUND(SGA_USED_MB/SGA_TOTAL_MB,2)  SGA_USED_PCT
		  ,ROUND(DATABASE_BUFFER_MB,2)  DATABASE_BUFFER_MB
		  ,ROUND(FIXED_SIZE_MB,2)  FIXED_SIZE_MB
		  ,ROUND(REDO_BUFFERS_MB,2)  REDO_BUFFERS_MB
		  ,ROUND(VARIABLE_SIZE_MB,2)  VARIABLE_SIZE_MB
		  ,ROUND(SHARED_POOL_TOTAL_MB,2)  SHARED_POOL_TOTAL_MB
		  ,ROUND(SHARED_POOL_USED_MB,2)  SHARED_POOL_USED_MB
		  ,ROUND(SHARED_POOL_FREE_MB,2)  SHARED_POOL_FREE_MB
	FROM
	(SELECT
		NVL(MAX(CASE NAME WHEN 'Database Buffers'  THEN VALUE END),0)/1024/1024 DATABASE_BUFFER_MB,
		NVL(MAX(CASE NAME WHEN 'Fixed Size'  THEN VALUE END),0)/1024/1024 FIXED_SIZE_MB,
		NVL(MAX(CASE NAME WHEN 'Redo Buffers' THEN VALUE END),0)/1024/1024 REDO_BUFFERS_MB,
		NVL(MAX(CASE NAME WHEN 'Variable Size' THEN VALUE END),0)/1024/1024 VARIABLE_SIZE_MB,
		NVL(SUM(VALUE),0)/1024/1024 SGA_TOTAL_MB
	FROM V$SGA),
	(SELECT
		SUM(CASE WHEN NAME  = 'free memory' THEN BYTES END)/1024/1024 SGA_FREE_MB,
		SUM(CASE WHEN NAME != 'free memory' THEN BYTES END)/1024/1024 SGA_USED_MB,
		SUM(CASE WHEN POOL = 'shared pool' THEN BYTES END)/1024/1024 SHARED_POOL_TOTAL_MB,
		SUM(CASE WHEN POOL = 'shared pool' AND NAME != 'free memory' THEN BYTES END)/1024/1024  SHARED_POOL_USED_MB,
		SUM(CASE WHEN POOL = 'shared pool' AND NAME  = 'free memory' THEN BYTES END)/1024/1024  SHARED_POOL_FREE_MB
	FROM V$SGASTAT)	
	`

	databaseInstanceSQLStatsQuery = `
	SELECT SQL_ID
	    ,SUBSTR(SQL_TEXT,1,100) SQL_TEXT
		,PARSE_CALLS
		,DISK_READS
		,DIRECT_WRITES
		,BUFFER_GETS
		,ROWS_PROCESSED
		,SERIALIZABLE_ABORTS
		,FETCHES
		,EXECUTIONS
		,END_OF_FETCH_COUNT
		,LOADS
		,VERSION_COUNT
		,INVALIDATIONS
		,PX_SERVERS_EXECUTIONS
		,CPU_TIME CPU_TIME_MICRO
		,ELAPSED_TIME ELAPSED_TIME_MICRO
		,AVG_HARD_PARSE_TIME AVG_HARD_PARSE_TIME_MICRO
		,APPLICATION_WAIT_TIME APPLICATION_WAIT_TIME_MICRO
		,CONCURRENCY_WAIT_TIME CONCURRENCY_WAIT_TIME_MICRO
		,CLUSTER_WAIT_TIME CLUSTER_WAIT_TIME_MICRO
		,USER_IO_WAIT_TIME USER_IO_WAIT_TIME_MICRO
		,PLSQL_EXEC_TIME PLSQL_EXEC_TIME_MICRO
		,JAVA_EXEC_TIME JAVA_EXEC_TIME_MICRO
		,SORTS
		,SHARABLE_MEM SHARABLE_MEM_BYTES
		,TOTAL_SHARABLE_MEM TOTAL_SHARABLE_MEM_BYTES
		,PHYSICAL_READ_REQUESTS
		,PHYSICAL_READ_BYTES
		,PHYSICAL_WRITE_REQUESTS
		,PHYSICAL_WRITE_BYTES
	FROM V$SQLSTATS
	WHERE LAST_ACTIVE_TIME > SYSDATE - NUMTODSINTERVAL(1, 'MINUTE')
	`

	databaseInstanceSysStatQuery = `
	SELECT STATISTIC#
		,CASE
          WHEN CLASS = 1 THEN 'User'
          WHEN CLASS = 2 THEN 'Redo'
          WHEN CLASS = 4 THEN 'Enqueue'
          WHEN CLASS = 8 THEN 'Cache'
          WHEN CLASS = 16 THEN 'OS'
          WHEN CLASS = 32 THEN 'Real Application Clusters'
          WHEN CLASS = 64 THEN 'SQL'
          WHEN CLASS = 128 THEN 'Debug'
          ELSE 'OTHER'
        END CLASS_NAME
	    ,REPLACE(REPLACE(REPLACE(REPLACE(NAME,' ','_'),'(',''),')',''),'-_','') NAME
		,CASE
		   WHEN NAME LIKE 'physical%'   THEN 'physical_io_stats'
		   WHEN NAME LIKE 'logical%'    THEN 'logical_io_stats'
		   WHEN NAME LIKE 'db block%'   THEN 'db_block_stats'
		   WHEN NAME LIKE 'consistent%' THEN 'consistent_block_stats'
		   WHEN NAME LIKE '%wait time'  THEN 'wait_time_stats'
		   WHEN NAME LIKE 'lob%'        THEN 'lob_stats'
		   WHEN NAME LIKE 'securefile%' THEN 'securefile_stats'
		   WHEN NAME LIKE 'enqueue%'    THEN 'enqueue_stats'
		   WHEN NAME LIKE 'commit%'     THEN 'commit_stats'
		   WHEN NAME LIKE 'table%'      THEN 'table_stats'
		   WHEN NAME LIKE 'index%'      THEN 'index_stats'
		   WHEN NAME LIKE 'parse%'      THEN 'parse_stats'
		   WHEN NAME LIKE 'sorts%'      THEN 'sorts_stats'
		   WHEN NAME LIKE 'bytes%'      THEN 'sql_net_stats'
		   WHEN NAME LIKE 'logons%'     THEN 'logons_stats'
		   WHEN NAME LIKE 'opened cursors%' THEN 'opened_cursors_stats'
		   WHEN NAME LIKE 'workarea%'   THEN 'workarea_stats'
		   WHEN NAME LIKE '%session%'   THEN 'session_stats'
		   WHEN NAME LIKE '%consistent read%'   THEN 'consistent_read_stats'
		   WHEN (NAME = 'execute count'     OR
		         NAME = 'Effective IO time' OR
				 NAME = 'DB time'           OR
				 NAME = 'file io service time' OR
				 NAME = 'Number of read IOs issued') THEN 'general_stats'
		   WHEN (NAME = 'free buffer requested'     OR
		         NAME = 'dirty buffers inspected' OR
				 NAME = 'pinned buffers inspected'           OR
				 NAME = 'hot buffers moved to head of LRU' OR
				 NAME = 'free buffer inspected') THEN 'buffer_stats'
		   WHEN NAME LIKE 'redo%' AND NOT NAME LIKE '%wait time' THEN 'redo_stats'
		   WHEN NAME LIKE '%user%' AND NOT NAME LIKE '%wait time' AND NOT NAME LIKE 'Workload%' THEN 'user_stats'
           ELSE 'other_stats'
		END STAT_TYPE
		,VALUE
	FROM V$SYSSTAT
	WHERE (NAME LIKE 'physical%'   OR NAME LIKE 'logical%'    OR
		 NAME LIKE 'db block%'   OR NAME LIKE 'consistent%' OR
		 NAME LIKE '%wait time'  OR NAME LIKE 'lob%'        OR
		 NAME LIKE 'securefile%' OR NAME LIKE 'enqueue%'    OR
		 NAME LIKE 'commit%'     OR NAME LIKE 'table%'      OR
		 NAME LIKE 'index%'      OR NAME LIKE 'parse%'      OR
		 NAME LIKE 'sorts%'      OR NAME LIKE 'bytes%'      OR 
		 NAME LIKE 'logons%'     OR NAME LIKE 'opened cursors%' OR
		 NAME LIKE 'workarea%'   OR NAME LIKE '%session%'   OR
		 NAME LIKE '%consistent read%'   OR
		 (NAME = 'execute count'     OR NAME = 'Effective IO time' OR
		  NAME = 'DB time'           OR NAME = 'file io service time' OR
		  NAME = 'Number of read IOs issued' OR NAME = 'free buffer requested'     OR
		  NAME = 'dirty buffers inspected'   OR NAME = 'pinned buffers inspected'  OR
		  NAME = 'hot buffers moved to head of LRU' OR NAME = 'free buffer inspected') OR
		 NAME LIKE 'redo%'       OR NAME LIKE '%user%')
		 AND NOT NAME LIKE 'Workload%'
	`

	databaseInstanceSystemEventQuery = `
	SELECT   
		WAIT_CLASS WAIT_CLASS,
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(NAME,' ','_'),'(',''),')',''),'-_',''),':','') EVENT_NAME,
		ROUND (TIME_WAITED_MICRO, 2) TIME_WAITED_MICRO
	FROM
		(SELECT
			N.WAIT_CLASS,
			E.EVENT NAME,
			E.TIME_WAITED_MICRO TIME_WAITED_MICRO
		FROM
			V$SYSTEM_EVENT E,
			V$EVENT_NAME N
		WHERE
			N.NAME = E.EVENT AND N.WAIT_CLASS <> 'Idle'
		AND
			TIME_WAITED > 0
		UNION
		SELECT
			'CPU',
			STAT_NAME,
			VALUE TIME_WAITED_MICRO
		FROM V$SYS_TIME_MODEL
		WHERE
		STAT_NAME IN ('background cpu time', 'DB CPU'))
	ORDER BY TIME_WAITED_MICRO DESC
	`

	databaseInstanceSysTimeModelQuery = `
    SELECT
	  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(STAT_NAME,' ','_'),'(',''),')',''),'-_',''),':','') STAT_NAME,
      VALUE TIME_MICRO
    FROM V$SYS_TIME_MODEL
    WHERE VALUE > 0 
	`
)

func (m *OracleDB) getConnection(serv string) (*go_ora.Connection, error) {

	db, err := go_ora.NewConnection(serv)

	if err != nil {
		return nil, err
	}

	err = db.Open()

	if err != nil {
		return nil, err
	}

	return db, nil
}

func (m *OracleDB) gatherServer(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	err := m.gatherDatabaseInstanceInfo(oi, db)
	if err != nil {
		return err
	}

	if m.GatherDatabaseInstanceDetails {
		err = m.gatherDatabaseInstanceDetails(oi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDatabaseInstanceSysmetric {
		err = m.gatherDatabaseInstanceSysmetric(oi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDatabaseInstanceWaitStats {
		err = m.gatherDatabaseInstanceWaitStats(oi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDatabaseInstanceWaitClassStats {
		err = m.gatherDatabaseInstanceWaitClassStats(oi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDatabaseInstanceUserSessionsDetails {
		err = m.gatherDatabaseInstanceUserSessionsDetails(oi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDatabaseInstanceUserSessions {
		err = m.gatherDatabaseInstanceUserSessions(oi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDatabaseInstanceTablespaces {
		err = m.gatherDatabaseInstanceTablespaces(oi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDatabaseInstanceSGA {
		err = m.gatherDatabaseInstanceSGA(oi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDatabaseInstanceSQLStats {
		err = m.gatherDatabaseInstanceSQLStats(oi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDatabaseInstanceSysStat {
		err = m.gatherDatabaseInstanceSysStat(oi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDatabaseInstanceSystemEvent {
		err = m.gatherDatabaseInstanceSystemEvent(oi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDatabaseInstanceSysTimeModel {
		err = m.gatherDatabaseInstanceSysTimeModel(oi, db, acc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceInfo(oi *OracleInstance, db *go_ora.Connection) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceInfoQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		instance_name   string
		instance_number int
		hostname        string
		container_name  string
	)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&instance_name,
			&instance_number,
			&hostname,
			&container_name); err == nil {

			oi.host = hostname
			oi.instance_name = instance_name
			oi.instance_number = instance_number

			if len(container_name) > 0 {
				oi.container_name = container_name
				oi.pluglable_database_name = instance_name
			}

		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceDetails(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceDetailsQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"startup_secs": 0,
	}

	var (
		instance_name   string
		instance_number int
		hostname        string
		instance_status string
		database_status string
		instance_role   string
		version         string
		edition         string
		database_type   string
		startup_secs    float64
	)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&instance_name,
			&instance_number,
			&hostname,
			&instance_status,
			&database_status,
			&instance_role,
			&version,
			&edition,
			&database_type,
			&startup_secs); err == nil {

			tags["db"] = oi.instance_name
			tags["instance_name"] = oi.instance_name
			tags["container_name"] = oi.container_name
			tags["pluglable_database_name"] = oi.pluglable_database_name
			tags["instance_number"] = strconv.Itoa(oi.instance_number)
			tags["host"] = oi.host
			tags["instance_status"] = instance_status
			tags["database_status"] = database_status
			tags["instance_role"] = instance_role
			tags["version"] = version
			tags["edition"] = edition
			tags["database_type"] = database_type
			fields["startup_secs"] = startup_secs

			if instance_status == "OPEN" {
				fields["status"] = 1.0
			} else {
				fields["status"] = 0.0
			}

			acc.AddFields("oracle_instance", fields, tags)

		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceSysmetric(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceSysmetricQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"metric_value": 0,
	}

	var (
		metric_name  string
		metric_unit  string
		metric_value float64
	)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&metric_name,
			&metric_value,
			&metric_unit); err == nil {

			tags["db"] = oi.instance_name
			tags["instance_name"] = oi.instance_name
			tags["container_name"] = oi.container_name
			tags["pluglable_database_name"] = oi.pluglable_database_name
			tags["instance_number"] = strconv.Itoa(oi.instance_number)
			tags["host"] = oi.host
			tags["metric_name"] = metric_name
			tags["metric_unit"] = metric_unit
			fields["metric_value"] = metric_value

			acc.AddFields("oracle_sysmetric", fields, tags)

		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceWaitStats(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceWaitStatsQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"latency": 0.0,
		"count":   0.0,
	}

	var (
		wait_class string
		wait_event string
		wait_count float64
		latency_ms float64
	)

	//num_rows := len(rows.Columns())
	//m.Log.Infof("Into m.gatherDatabaseInstanceWaitStats %v", num_rows)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&wait_class,
			&wait_event,
			&wait_count,
			&latency_ms); err == nil {

			tags["db"] = oi.instance_name
			tags["instance_name"] = oi.instance_name
			tags["container_name"] = oi.container_name
			tags["pluglable_database_name"] = oi.pluglable_database_name
			tags["instance_number"] = strconv.Itoa(oi.instance_number)
			tags["host"] = oi.host
			tags["wait_class"] = wait_class
			tags["wait_event"] = wait_event
			fields["latency"] = latency_ms
			fields["count"] = wait_count

			acc.AddFields("oracle_wait_event", fields, tags)

		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceWaitClassStats(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceWaitClassStatsQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"wait_value": 0.0,
	}

	var (
		wait_class string
		wait_value float64
	)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&wait_class,
			&wait_value); err == nil {

			tags["db"] = oi.instance_name
			tags["instance_name"] = oi.instance_name
			tags["container_name"] = oi.container_name
			tags["pluglable_database_name"] = oi.pluglable_database_name
			tags["instance_number"] = strconv.Itoa(oi.instance_number)
			tags["host"] = oi.host
			tags["wait_class"] = wait_class
			fields["wait_value"] = wait_value

			acc.AddFields("oracle_wait_class", fields, tags)

		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceUserSessionsDetails(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceUserSessionsDetailsQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"last_call_et_min": 0.0,
		"last_call_et_max": 0.0,
		"last_call_et_avg": 0.0,
		"wait_time_min":    0.0,
		"wait_time_max":    0.0,
		"wait_time_avg":    0.0,
		"count":            0,
	}

	var (
		username         string
		status           string
		terminal         string
		event            string
		sql_id           string
		lockwait         string
		command          string
		last_call_et_min float64
		last_call_et_max float64
		last_call_et_avg float64
		wait_time_min    float64
		wait_time_max    float64
		wait_time_avg    float64
		count            int64
	)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&username,
			&status,
			&terminal,
			&event,
			&sql_id,
			&lockwait,
			&command,
			&last_call_et_min,
			&last_call_et_max,
			&last_call_et_avg,
			&wait_time_min,
			&wait_time_max,
			&wait_time_avg,
			&count); err == nil {

			tags["db"] = oi.instance_name
			tags["instance_name"] = oi.instance_name
			tags["container_name"] = oi.container_name
			tags["pluglable_database_name"] = oi.pluglable_database_name
			tags["instance_number"] = strconv.Itoa(oi.instance_number)
			tags["host"] = oi.host
			tags["username"] = username
			tags["status"] = status
			tags["terminal"] = terminal
			tags["event"] = event
			tags["sql_id"] = sql_id
			tags["lockwait"] = lockwait
			tags["command"] = command
			fields["last_call_et_min"] = last_call_et_min
			fields["last_call_et_max"] = last_call_et_max
			fields["last_call_et_avg"] = last_call_et_avg
			fields["wait_time_min"] = wait_time_min
			fields["wait_time_max"] = wait_time_max
			fields["wait_time_avg"] = wait_time_avg
			fields["count"] = count

			acc.AddFields("oracle_session_user_detail", fields, tags)

		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceUserSessions(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceUserSessionsQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"last_call_et_min": 0.0,
		"last_call_et_max": 0.0,
		"last_call_et_avg": 0.0,
		"count":            0,
	}

	var (
		username         string
		status           string
		last_call_et_min float64
		last_call_et_max float64
		last_call_et_avg float64
		count            int64
	)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&username,
			&status,
			&last_call_et_min,
			&last_call_et_max,
			&last_call_et_avg,
			&count); err == nil {

			tags["db"] = oi.instance_name
			tags["instance_name"] = oi.instance_name
			tags["container_name"] = oi.container_name
			tags["pluglable_database_name"] = oi.pluglable_database_name
			tags["instance_number"] = strconv.Itoa(oi.instance_number)
			tags["host"] = oi.host
			tags["username"] = username
			tags["status"] = status
			fields["last_call_et_min"] = last_call_et_min
			fields["last_call_et_max"] = last_call_et_max
			fields["last_call_et_avg"] = last_call_et_avg
			fields["count"] = count

			acc.AddFields("oracle_session_user", fields, tags)

		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceTablespaces(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceTablespacesQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"total_space_mb":           0.0,
		"free_space_mb":            0.0,
		"percent_used":             0.0,
		"percent_used_autoext":     0.0,
		"max_size_mb":              0.0,
		"free_space_autoextend_mb": 0.0,
	}

	var (
		tbs_name                 string
		total_space_mb           float64
		free_space_mb            float64
		percent_used             float64
		percent_used_autoext     float64
		max_size_mb              float64
		free_space_autoextend_mb float64
	)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&tbs_name,
			&total_space_mb,
			&free_space_mb,
			&percent_used,
			&percent_used_autoext,
			&max_size_mb,
			&free_space_autoextend_mb); err == nil {

			tags["db"] = oi.instance_name
			tags["instance_name"] = oi.instance_name
			tags["container_name"] = oi.container_name
			tags["pluglable_database_name"] = oi.pluglable_database_name
			tags["instance_number"] = strconv.Itoa(oi.instance_number)
			tags["host"] = oi.host
			tags["tbs_name"] = tbs_name
			fields["total_space_mb"] = total_space_mb
			fields["free_space_mb"] = free_space_mb
			fields["percent_used"] = percent_used
			fields["percent_used_autoext"] = percent_used_autoext
			fields["max_size_mb"] = max_size_mb
			fields["free_space_autoextend_mb"] = free_space_autoextend_mb

			acc.AddFields("oracle_tablespace", fields, tags)

		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceSGA(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceSGAQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"sga_total_mb": 0.0,
		"sga_free_mb":  0.0,
		"sga_used_mb":  0.0,
	}

	var (
		sga_total_mb         float64
		sga_free_mb          float64
		sga_used_mb          float64
		sga_used_pct         float64
		database_buffer_mb   float64
		fixed_size_mb        float64
		redo_buffers_mb      float64
		variable_size_mb     float64
		shared_pool_total_mb float64
		shared_pool_free_mb  float64
		shared_pool_used_mb  float64
	)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&sga_total_mb,
			&sga_free_mb,
			&sga_used_mb,
			&sga_used_pct,
			&database_buffer_mb,
			&fixed_size_mb,
			&redo_buffers_mb,
			&variable_size_mb,
			&shared_pool_total_mb,
			&shared_pool_free_mb,
			&shared_pool_used_mb); err == nil {

			tags["db"] = oi.instance_name
			tags["instance_name"] = oi.instance_name
			tags["container_name"] = oi.container_name
			tags["pluglable_database_name"] = oi.pluglable_database_name
			tags["instance_number"] = strconv.Itoa(oi.instance_number)
			tags["host"] = oi.host
			fields["sga_total_mb"] = sga_total_mb
			fields["sga_free_mb"] = sga_free_mb
			fields["sga_used_mb"] = sga_used_mb
			fields["sga_used_pct"] = sga_used_pct
			fields["database_buffer_mb"] = database_buffer_mb
			fields["fixed_size_mb"] = fixed_size_mb
			fields["redo_buffers_mb"] = redo_buffers_mb
			fields["variable_size_mb"] = variable_size_mb
			fields["shared_pool_total_mb"] = shared_pool_total_mb
			fields["shared_pool_free_mb"] = shared_pool_free_mb
			fields["shared_pool_used_mb"] = shared_pool_used_mb

			acc.AddFields("oracle_sga", fields, tags)

		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceSQLStats(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceSQLStatsQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"parse_calls":                 0.0,
		"disk_reads":                  0.0,
		"direct_writes":               0.0,
		"buffer_gets":                 0.0,
		"rows_processed":              0.0,
		"serializable_aborts":         0.0,
		"fetches":                     0.0,
		"executions":                  0.0,
		"end_of_fetch_count":          0.0,
		"loads":                       0.0,
		"version_count":               0.0,
		"invalidations":               0.0,
		"px_servers_executions":       0.0,
		"cpu_time_micro":              0.0,
		"elapsed_time_micro":          0.0,
		"avg_hard_parse_time_micro":   0.0,
		"application_wait_time_micro": 0.0,
		"concurrency_wait_time_micro": 0.0,
		"cluster_wait_time_micro":     0.0,
		"user_io_wait_time_micro":     0.0,
		"plsql_exec_time_micro":       0.0,
		"java_exec_time_micro":        0.0,
		"sorts":                       0.0,
		"sharable_mem_bytes":          0.0,
		"total_sharable_mem_bytes":    0.0,
		"physical_read_requests":      0.0,
		"physical_read_bytes":         0.0,
		"physical_write_requests":     0.0,
		"physical_write_bytes":        0.0,
	}

	var (
		sql_id                      string
		sql_text                    string
		parse_calls                 float64
		disk_reads                  float64
		direct_writes               float64
		buffer_gets                 float64
		rows_processed              float64
		serializable_aborts         float64
		fetches                     float64
		executions                  float64
		end_of_fetch_count          float64
		loads                       float64
		version_count               float64
		invalidations               float64
		px_servers_executions       float64
		cpu_time_micro              float64
		elapsed_time_micro          float64
		avg_hard_parse_time_micro   float64
		application_wait_time_micro float64
		concurrency_wait_time_micro float64
		cluster_wait_time_micro     float64
		user_io_wait_time_micro     float64
		plsql_exec_time_micro       float64
		java_exec_time_micro        float64
		sorts                       float64
		sharable_mem_bytes          float64
		total_sharable_mem_bytes    float64
		physical_read_requests      float64
		physical_read_bytes         float64
		physical_write_requests     float64
		physical_write_bytes        float64
	)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&sql_id,
			&sql_text,
			&parse_calls,
			&disk_reads,
			&direct_writes,
			&buffer_gets,
			&rows_processed,
			&serializable_aborts,
			&fetches,
			&executions,
			&end_of_fetch_count,
			&loads,
			&version_count,
			&invalidations,
			&px_servers_executions,
			&cpu_time_micro,
			&elapsed_time_micro,
			&avg_hard_parse_time_micro,
			&application_wait_time_micro,
			&concurrency_wait_time_micro,
			&cluster_wait_time_micro,
			&user_io_wait_time_micro,
			&plsql_exec_time_micro,
			&java_exec_time_micro,
			&sorts,
			&sharable_mem_bytes,
			&total_sharable_mem_bytes,
			&physical_read_requests,
			&physical_read_bytes,
			&physical_write_requests,
			&physical_write_bytes); err == nil {

			tags["db"] = oi.instance_name
			tags["instance_name"] = oi.instance_name
			tags["container_name"] = oi.container_name
			tags["pluglable_database_name"] = oi.pluglable_database_name
			tags["instance_number"] = strconv.Itoa(oi.instance_number)
			tags["host"] = oi.host
			tags["sql_id"] = sql_id
			tags["sql_text"] = sql_text
			fields["parse_calls"] = parse_calls
			fields["disk_reads"] = disk_reads
			fields["direct_writes"] = direct_writes
			fields["buffer_gets"] = buffer_gets
			fields["rows_processed"] = rows_processed
			fields["serializable_aborts"] = serializable_aborts
			fields["fetches"] = fetches
			fields["executions"] = executions
			fields["end_of_fetch_count"] = end_of_fetch_count
			fields["loads"] = loads
			fields["version_count"] = version_count
			fields["invalidations"] = invalidations
			fields["px_servers_executions"] = px_servers_executions
			fields["cpu_time_micro"] = cpu_time_micro
			fields["elapsed_time_micro"] = elapsed_time_micro
			fields["avg_hard_parse_time_micro"] = avg_hard_parse_time_micro
			fields["application_wait_time_micro"] = application_wait_time_micro
			fields["concurrency_wait_time_micro"] = concurrency_wait_time_micro
			fields["cluster_wait_time_micro"] = cluster_wait_time_micro
			fields["user_io_wait_time_micro"] = user_io_wait_time_micro
			fields["plsql_exec_time_micro"] = plsql_exec_time_micro
			fields["java_exec_time_micro"] = java_exec_time_micro
			fields["sorts"] = sorts
			fields["sharable_mem_bytes"] = sharable_mem_bytes
			fields["total_sharable_mem_bytes"] = total_sharable_mem_bytes
			fields["physical_read_requests"] = physical_write_requests
			fields["physical_read_bytes"] = physical_read_bytes
			fields["physical_write_requests"] = physical_write_requests
			fields["physical_write_bytes"] = physical_write_bytes

			acc.AddFields("oracle_sqlstats", fields, tags)

		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceSysStat(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceSysStatQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"metric_value": 0.0,
	}

	var (
		statistic_number int
		class_name       string
		metric_name      string
		stat_type        string
		metric_value     float64
	)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&statistic_number,
			&class_name,
			&metric_name,
			&stat_type,
			&metric_value); err == nil {

			tags["db"] = oi.instance_name
			tags["instance_name"] = oi.instance_name
			tags["container_name"] = oi.container_name
			tags["pluglable_database_name"] = oi.pluglable_database_name
			tags["instance_number"] = strconv.Itoa(oi.instance_number)
			tags["host"] = oi.host
			tags["statistic_number"] = strconv.Itoa(statistic_number)
			tags["class_name"] = class_name
			tags["metric_name"] = metric_name
			tags["stat_type"] = stat_type
			fields["metric_value"] = metric_value

			acc.AddFields("oracle_sysstat", fields, tags)

		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceSystemEvent(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceSystemEventQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"time_waited_micro": 0.0,
	}

	var (
		class_name        string
		event_name        string
		time_waited_micro float64
	)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&class_name,
			&event_name,
			&time_waited_micro); err == nil {

			tags["db"] = oi.instance_name
			tags["instance_name"] = oi.instance_name
			tags["container_name"] = oi.container_name
			tags["pluglable_database_name"] = oi.pluglable_database_name
			tags["instance_number"] = strconv.Itoa(oi.instance_number)
			tags["host"] = oi.host
			tags["class_name"] = class_name
			tags["event_name"] = event_name
			fields["time_waited_micro"] = time_waited_micro

			acc.AddFields("oracle_system_event", fields, tags)
		}
	}

	return nil
}

func (m *OracleDB) gatherDatabaseInstanceSysTimeModel(oi *OracleInstance, db *go_ora.Connection, acc telegraf.Accumulator) error {

	//Create statement
	stmt := go_ora.NewStmt(databaseInstanceSysTimeModelQuery, db)
	defer stmt.Close()

	// run query
	rows, err := stmt.Query_(nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"time_micro": 0.0,
	}

	var (
		stat_name  string
		time_micro float64
	)

	// iterate over rows
	for rows.Next_() {
		if err := rows.Scan(&stat_name,
			&time_micro); err == nil {

			tags["db"] = oi.instance_name
			tags["instance_name"] = oi.instance_name
			tags["container_name"] = oi.container_name
			tags["pluglable_database_name"] = oi.pluglable_database_name
			tags["instance_number"] = strconv.Itoa(oi.instance_number)
			tags["host"] = oi.host
			tags["stat_name"] = stat_name
			fields["time_micro"] = time_micro

			acc.AddFields("oracle_sys_time_model", fields, tags)
		}
	}

	return nil
}

func init() {
	inputs.Add("oracledb", func() telegraf.Input {
		return &OracleDB{
			Server:                                    defaultServer,
			GatherDatabaseInstanceDetails:             defaultGatherDatabaseInstanceDetails,
			GatherDatabaseInstanceSysmetric:           defaultGatherDatabaseInstanceSysmetric,
			GatherDatabaseInstanceWaitStats:           defaultGatherDatabaseInstanceWaitStats,
			GatherDatabaseInstanceWaitClassStats:      defaultGatherDatabaseInstanceWaitClassStats,
			GatherDatabaseInstanceUserSessions:        defaultGatherDatabaseInstanceUserSessions,
			GatherDatabaseInstanceUserSessionsDetails: defaultGatherDatabaseInstanceUserSessionsDetails,
			GatherDatabaseInstanceTablespaces:         defaultGatherDatabaseInstanceTablespaces,
			GatherDatabaseInstanceSGA:                 defaultGatherDatabaseInstanceSGA,
			GatherDatabaseInstanceSQLStats:            defaultGatherDatabaseInstanceSQLStats,
			GatherDatabaseInstanceSysStat:             defaultGatherDatabaseInstanceSysStat,
			GatherDatabaseInstanceSystemEvent:         defaultGatherDatabaseInstanceSystemEvent,
			GatherDatabaseInstanceSysTimeModel:        defaultGatherDatabaseInstanceSysTimeModel,
		}
	})
}
