// Package rand is loosely based off of https://github.com/danielnelson/telegraf-plugins
package hanadb

import (
	"database/sql"
	"fmt"
	"math/big"
	"net/url"
	"strings"
	"sync"

	"github.com/SAP/go-hdb/driver"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type HanaDB struct {
	Server                                 string   `toml:"server"`
	User                                   string   `toml:"user"`
	Password                               string   `toml:"password"`
	GatherTenanInstances                   bool     `toml:"gather_tenan_instances"`
	GatherDatabaseDetails                  bool     `toml:"gather_database_details"`
	GatherServiceMemory                    bool     `toml:"gather_service_memory"`
	GatherServicePorts                     bool     `toml:"gather_service_ports"`
	GatherConnectionStats                  bool     `toml:"gather_connection_stats"`
	GatherSqlServiceStats                  bool     `toml:"gather_sql_service_stats"`
	GatherSchemaMemory                     bool     `toml:"gather_schema_memory"`
	CollectInternalSchemaMemory            bool     `toml:"collect_internal_schema_memory"`
	InternalSchemaPrefix                   []string `toml:"internal_schema_prefix"`
	InternalSchemaSuffix                   []string `toml:"internal_schema_suffix"`
	GatherHostResourceUtilization          bool     `toml:"gather_host_resource_utilization"`
	GatherHostDiskUsage                    bool     `toml:"gather_host_disk_usage"`
	GatherServiceReplication               bool     `toml:"gather_service_replication"`
	GatherCurrentAlerts                    bool     `toml:"gather_current_alerts"`
	GatherHostAgentCPUMetrics              bool     `toml:"gather_host_agent_cpu_metrics"`
	GatherHostAgentNetworkMetrics          bool     `toml:"gather_host_agent_network_metrics"`
	GatherDiskDataFiles                    bool     `toml:"gather_disk_data_files"`
	GatherDiskIO                           bool     `toml:"gather_disk_io"`
	GatherInstanceWorkload                 bool     `toml:"gather_instance_workload"`
	GatherSystemDBDatabases                bool     `toml:"gather_systemdb_databases"`
	GatherTopHeapMemoryCategories          bool     `toml:"gather_top_heap_memory_categories"`
	TopHeapMemoryCategories                int      `toml:"top_heap_memory_categories"`
	GatherSDIRemoteSubscriptionsStadistics bool     `toml:"gather_sdi_remote_subscriptions_stadistics"`
	GatherLicenseUsage                     bool     `toml:"gather_license_usage"`
	GatherServiceBufferCacheStats          bool     `toml:"gather_service_buffer_cache_stats"`
	GatherServiceThreads                   bool     `toml:"gather_service_threads"`
	GatherSDIRemoteSourceStatistics        bool     `toml:"gather_sdi_remote_source_statistics"`

	Log telegraf.Logger `toml:"-"`
	//InstanceInfo           map[string]interface{}
}

type HanaInstance struct {
	url             string
	host            string
	instance_id     string
	instance_number string
	database_name   string
	//internal_schemas_memory_used_mb float64
	//user_schemas_memory_used_mb float64
}

const sampleConfig = `
  ## specify servers via a url matching:
  ##  [username[:password]@][protocol[(address)]]/[?tls=[true|false|skip-verify|custom]]
  ##  see https://github.com/go-sql-driver/mysql#dsn-data-source-name
  ##  e.g.
  ##    servers = ["user:passwd@tcp(127.0.0.1:3306)/?tls=false"]
  ##    servers = ["user@tcp(127.0.0.1:3306)/?tls=false"]
  #
  ## If no servers are specified, then localhost is used as the host.
  # server = "localhost:30013"
  # user = "user"
  # password = "password"
  ## If database is SYSTEMDB and user/password are same for tenan enable gathering 
  ## Note: Asume network resolution to host:port of tenans
  # gather_tenan_instances = true

  # gather_database_details = true
  # gather_service_memory = true
  # gather_service_ports = true
  # gather_connection_stats = true
  # gather_sql_service_stats = true
  # gather_schema_memory = true
  # collect_internalschema_memory = true
  ## For collect_internalschema_memory, internal_schema options to categorize schemas (defaults will be fine)
  # internal_schema_prefix = ["_SYS", "HANA_", "SYS", "XSS", "XSA", "SAP", "USR", "HDI", "UIS"]
  # internal_schema_sufffix = ["TEST"]
  # gather_host_resource_utilization = true
  # gather_host_disk_usage = true
  ## Recommend collect every 5 minutes
  # gather_service_replication = true
  # gather_current_alerts = true
  # gather_host_agent_cpu_metrics = true
  # gather_host_agent_network_metrics = true
  # gather_disk_data_files = true
  # gather_disk_io = true
  # gather_instance_workload = true
  # gather_systemdb_databases = true
  # gather_top_heap_memory_categories = true
  ## Change top of categories in heap memory
  # top_heap_memory_categories = 10
  # gather_sdi_remote_subscriptions_stadistics = true
  # gather_license_usage = true
  # gather_service_buffer_cache_stats = true
  # gather_service_threads = true
  # gather_sdi_remote_source_statistics = true
`

const (
	defaultServer                                 = "localhost:30013"
	defaultGatherTenanInstances                   = false
	defaultGatherDatabaseDetails                  = false
	defaultGatherServiceMemory                    = false
	defaultGatherServicePorts                     = false
	defaultGatherConnectionStats                  = false
	defaultGatherSqlServiceStats                  = false
	defaultGatherSchemaMemory                     = false
	defaultCollectInternalSchemaMemory            = false
	defaultGatherHostResourceUtilization          = false
	defaultGatherHostDiskUsage                    = false
	defaultGatherServiceReplication               = false
	defaultGatherCurrentAlerts                    = false
	defaultGatherHostAgentCPUMetrics              = false
	defaultGatherHostAgentNetworkMetrics          = false
	defaultGatherDiskDataFiles                    = false
	defaultGatherDiskIO                           = false
	defaultGatherInstanceWorkload                 = false
	defaultGatherSystemDBDatabases                = false
	defaultGatherTopHeapMemoryCategories          = false
	defaultTopHeapMemoryCategories                = 10
	defaultGatherSDIRemoteSubscriptionsStadistics = false
	defaultGatherLicenseUsage                     = false
	defaultGatherServiceBufferCacheStats          = false
	defaultGatherServiceThreads                   = false
	defaultGatherSDIRemoteSourceStatistics        = false
)

func (m *HanaDB) Init() error {
	if m.InternalSchemaPrefix == nil {
		m.InternalSchemaPrefix = []string{"_SYS", "HANA_", "SYS", "XSS", "XSA", "SAP", "USR", "HDI", "UIS"}
	}
	if m.GatherTenanInstances {
		m.Log.Infof("garther_tenan_instances enabled, please verify same user/password over all tenans and network conectivity from agent collector.")
	}
	/*if m.InternalSchemaSuffix == nil {
		m.InternalSchemaSuffix  = []string{"#DI"}
	}*/
	//m.Config = make(map[string]interface{})
	//m.Config["Prueba"] = "Prueba"
	//m.Log.Infof("Init call: %s", m.Config["Prueba"])
	return nil
}

func (m *HanaDB) SampleConfig() string {
	return sampleConfig
}

func (m *HanaDB) Description() string {
	return "Read metrics from SAP Hana Database"
}

//const localhost = ""

func (m *HanaDB) Gather(acc telegraf.Accumulator) error {
	//var servers []string

	/*if len(m.Server) == 0 {
		// default to localhost if nothing specified.
		return m.gatherServer(localhost, acc)
	}*/
	// Initialise additional query intervals
	//servers = append(servers,m.Server)
	var wg sync.WaitGroup

	userurl := url.UserPassword(m.User, m.Password).String()

	hdbUrl := fmt.Sprintf("hdb://%s@%s", userurl, m.Server)

	db, err := m.getConnection(hdbUrl)
	if err != nil {
		return err
	}

	defer db.Close()

	/*wg.Add(1)
	go func(s string) {
		defer wg.Done()
		acc.AddError(m.gatherServer(db, acc))
	}(hdb_url)*/
	var instanceInfo HanaInstance
	acc.AddError(m.gatherServer(&instanceInfo, db, acc))

	if m.GatherTenanInstances && instanceInfo.database_name == "SYSTEMDB" {
		tenanList, err := m.getTenanInstances(db)
		if err != nil {
			return err
		}
		//m.Log.Infof("%s", tenanList)
		// Loop through each server and collect metrics
		for _, tenanInfo := range tenanList {
			hdbUrl = fmt.Sprintf("hdb://%s@%s", userurl, tenanInfo.url)
			//m.Log.Infof("Collect metrics for tenan: %s", server)
			wg.Add(1)

			/*dbtenan, err := m.getConnection(hdbUrl)
			if err != nil {
				return err
			}

			defer dbtenan.Close()
			acc.AddError(m.gatherServer(&tenanInfo, dbtenan, acc))*/
			err = func(s string) error {
				defer wg.Done()
				dbtenan, err := m.getConnection(s)
				if err != nil {
					return err
				}

				defer dbtenan.Close()
				acc.AddError(m.gatherServer(&tenanInfo, dbtenan, acc))
				return nil
			}(hdbUrl)
			if err != nil {
				return err
			}
		}
	}

	wg.Wait()
	return nil
}

// metric queries
const (
	databaseInstanceInfoQuery = `
        SELECT A.DATABASE_NAME
              ,A.HOST
              ,B.InstanceId
              ,B.InstanceNumber
        FROM
        (SELECT A.DATABASE_NAME DATABASE_NAME
               ,A.HOST HOST
        FROM SYS.M_DATABASE  A
            ,SYS.M_CS_TABLES B
        WHERE A.HOST = B.HOST
        GROUP BY A.DATABASE_NAME, A.HOST, A.VERSION) A,
        (SELECT Id.VALUE InstanceId
            ,INum.VALUE InstanceNumber
        FROM 
            (SELECT VALUE FROM M_SYSTEM_OVERVIEW WHERE SECTION='System' AND NAME='Instance ID') Id,
            (SELECT VALUE FROM M_SYSTEM_OVERVIEW WHERE SECTION='System' AND NAME='Instance Number') INum) B
        ;
	`
	tenanInstancesQuery = `
	SELECT DATABASE_NAME,HOST,SQL_PORT
	FROM SYS_DATABASES.M_SERVICES
	WHERE SERVICE_NAME in ('nameserver','indexserver') AND COORDINATOR_TYPE='MASTER' AND DATABASE_NAME != 'SYSTEMDB';
	`
	databaseDetailsQuery = `
       SELECT A.HOST
              ,C.InstanceId
              ,C.InstanceNumber
              ,C.Distribuited
              ,C.Version
              ,C.Platform
              ,A.DATABASE_NAME
              ,A.UPTIME_SECS
              ,A.colum_tables_total_used_mb
              ,B.row_tables_fixedpart_used_mb
              ,B.row_tables_variablepart_used_mb
              ,B.row_tables_total_used_mb
              ,C.ServicesStatus
              ,C.MemoryStatus
              ,C.CPUStatus
              ,C.DiskDataStatus
              ,C.DiskLogStatus
              ,C.DiskTraceStatus
              ,C.StatsAlertStatus
        FROM
        (SELECT A.DATABASE_NAME DATABASE_NAME
               ,A.HOST HOST
               ,A.VERSION VERSION
               ,SECONDS_BETWEEN(MAX(A.START_TIME),CURRENT_TIMESTAMP) UPTIME_SECS
               ,ROUND(SUM(B.memory_size_in_total)/1024/1024,2) colum_tables_total_used_mb
        FROM SYS.M_DATABASE  A
            ,SYS.M_CS_TABLES B
        WHERE A.HOST = B.HOST
        GROUP BY A.DATABASE_NAME, A.HOST, A.VERSION) A,
        (SELECT A.DATABASE_NAME DATABASE_NAME
               ,A.HOST HOST
               ,ROUND(SUM(B.USED_FIXED_PART_SIZE)/1024/1024,2) row_tables_fixedpart_used_mb
               ,ROUND(SUM(B.USED_VARIABLE_PART_SIZE)/1024/1024,2) row_tables_variablepart_used_mb
               ,ROUND(SUM(B.USED_FIXED_PART_SIZE + B.USED_VARIABLE_PART_SIZE)/1024/1024,2) row_tables_total_used_mb
        FROM SYS.M_DATABASE  A
            ,SYS.M_RS_TABLES B
        WHERE A.HOST = B.HOST
        GROUP BY A.DATABASE_NAME, A.HOST) B,        
        (SELECT Id.VALUE InstanceId
            ,INum.VALUE InstanceNumber
            ,Dis.Value Distribuited
            ,Ver.Value Version
            ,Plat.Value Platform
            ,MAP(Services.Status,'OK',1,0) ServicesStatus
            ,MAP(Memory.Status,'OK',1,0) MemoryStatus
            ,MAP(Cpu.Status,'OK',1,0) CPUStatus
            ,MAP(DiskData.Status,'OK',1,0) DiskDataStatus
            ,MAP(DiskLog.Status,'OK',1,0) DiskLogStatus
            ,MAP(DiskTrace.Status,'OK',1,0) DiskTraceStatus
            ,MAP(StatsAlert.Status,'OK',1,0) StatsAlertStatus
        FROM
            (SELECT VALUE FROM M_SYSTEM_OVERVIEW WHERE SECTION='System' AND NAME='Instance ID') Id,
            (SELECT VALUE FROM M_SYSTEM_OVERVIEW WHERE SECTION='System' AND NAME='Instance Number') INum,
            (SELECT VALUE FROM M_SYSTEM_OVERVIEW WHERE SECTION='System' AND NAME='Distributed') Dis,
            (SELECT VALUE FROM M_SYSTEM_OVERVIEW WHERE SECTION='System' AND NAME='Version') Ver,
            (SELECT VALUE FROM M_SYSTEM_OVERVIEW WHERE SECTION='System' AND NAME='Platform') Plat,
            (SELECT STATUS FROM M_SYSTEM_OVERVIEW WHERE SECTION='Services' AND NAME='All Started') Services,
            (SELECT STATUS FROM M_SYSTEM_OVERVIEW WHERE SECTION='Memory' AND NAME='Memory') Memory,
            (SELECT STATUS FROM M_SYSTEM_OVERVIEW WHERE SECTION='CPU' AND NAME='CPU') cpu,
            (SELECT STATUS FROM M_SYSTEM_OVERVIEW WHERE SECTION='Disk' AND NAME='Data') DiskData,
            (SELECT STATUS FROM M_SYSTEM_OVERVIEW WHERE SECTION='Disk' AND NAME='Log') DiskLog,
            (SELECT STATUS FROM M_SYSTEM_OVERVIEW WHERE SECTION='Disk' AND NAME='Trace') DiskTrace,
            (SELECT STATUS FROM M_SYSTEM_OVERVIEW WHERE SECTION='Statistics' AND NAME='Alerts') StatsAlert
        ) C ;
	`
	serviceMemoryQuery = `
	SELECT m.host host
	      ,LPAD(m.port, 5) port
	      ,m.service_name service
	      ,TO_DECIMAL(m.shared_memory_allocated_size / 1024 / 1024, 10, 2) shm_alloc_mb
	      ,TO_DECIMAL(m.shared_memory_used_size / 1024 / 1024, 10, 2) shm_used_mb
	      ,TO_DECIMAL(MAP(m.shared_memory_allocated_size, 0, 0, m.shared_memory_used_size / m.shared_memory_allocated_size * 100), 10, 2) shm_used_pct
	      ,TO_DECIMAL(m.heap_memory_allocated_size / 1024 / 1024, 10, 2) heap_alloc_mb
	      ,TO_DECIMAL(m.heap_memory_used_size / 1024 / 1024, 10, 2) heap_used_mb
	      ,TO_DECIMAL(MAP(m.heap_memory_allocated_size, 0, 0, m.heap_memory_used_size / m.heap_memory_allocated_size * 100), 10, 2) heap_used_pct
	      ,TO_DECIMAL(m.total_memory_used_size / 1024 / 1024, 10, 2) total_memory_used_mb
	      ,TO_DECIMAL(m.physical_memory_size / 1024 / 1024, 10, 2) total_phys_mem_mb
	      ,TO_DECIMAL(m.logical_memory_size / 1024 / 1024, 10, 2) total_logical_mem_mb
	      ,TO_DECIMAL(m.code_size / 1024 / 1024, 10, 2) code_size_mem_mb
	      ,TO_DECIMAL(m.stack_size / 1024 / 1024, 10, 2) stack_size_mem_mb
	      ,TO_DECIMAL(m.compactors_freeable_size / 1024 / 1024, 10, 2) compactors_freeable_size_mem_mb
	      ,TO_DECIMAL(m.compactors_allocated_size / 1024 / 1024, 10, 2) compactors_allocated_size_mem_mb
	      ,TO_DECIMAL(m.allocation_limit / 1024 / 1024, 10, 2) process_alloc_limit_mb
	      ,TO_DECIMAL(m.effective_allocation_limit / 1024 / 1024, 10, 2) effective_proc_alloc_limit_mb 
	      ,ifnull ( TO_DECIMAL((mr.inclusive_peak_allocation_size + m.code_size + m.shared_memory_allocated_size)/ 1024 / 1024, 10, 2),
                        TO_DECIMAL(m.total_memory_used_size / 1024 / 1024, 10, 2)
                      ) inclusive_peak_allocation_size_mb
              ,ifnull (TO_DECIMAL(mh.heap_hex_mem_used / 1024 / 1024, 10, 2),0) heap_hex_mem_used_mb
	FROM sys.m_service_memory m join sys.m_heap_memory_reset mr
	 on ( m."HOST" = mr."HOST" AND m."PORT" = mr."PORT" AND mr."DEPTH" = 0)
        left join (select host, port, sum(exclusive_size_in_use) heap_hex_mem_used
            from m_heap_memory
            where category = 'Pool/L/llang/Debuggee'
            group by host, port) mh
         on m."HOST" = mh."HOST" AND m."PORT" = mh."PORT";
	`
	columnTablesMemoryQuery = `
	SELECT host, ROUND(SUM(memory_size_in_total)/1024/1024) column_tables_used_mb
        FROM sys.m_cs_tables
	GROUP BY host;
	`
	servicePortsQuery = `
	SELECT SERVICE_NAME, PORT, SQL_PORT, (PORT + 2) HTTP_PORT
        FROM SYS.M_SERVICES
	WHERE SERVICE_NAME in ('indexserver','nameserver','xsengine')
	;
	`
	connectionStatsQuery = `
	SELECT host, LPAD(port, 5) port, connection_type, MAP(connection_status,'','N/A', connection_status) connection_status, COUNT(1) total_connections
        FROM SYS.M_CONNECTIONS
	GROUP BY host, port, connection_status, connection_type;
	`
	sqlServiceStatsQuery = `
	SELECT HOST
	      ,LPAD(PORT, 5) PORT
	      ,SERVICE_NAME SERVICE
	      ,SQL_TYPE
	      ,EXECUTIONS EXECUTIONS
	      ,ROUND(ELAPSED_MS) ELAPSED_MS
	      ,TO_DECIMAL(ELA_PER_EXEC_MS, 10, 2) ELA_PER_EXEC_MS
	      ,TO_DECIMAL(LOCK_WAIT_TIME_MS, 10, 2) LOCK_WAIT_TIME_MS
	      ,TO_DECIMAL(LOCK_PER_EXEC_MS, 10, 2) LOCK_PER_EXEC_MS
	      ,ROUND(MAX_ELA_MS) MAX_ELA_MS
	      FROM
	       (SELECT S.HOST, S.PORT, S.SERVICE_NAME, L.SQL_TYPE
	              ,CASE L.SQL_TYPE 
		          WHEN 'SELECT'                THEN SUM(C.SELECT_EXECUTION_COUNT)
		          WHEN 'SELECT FOR UPDATE'     THEN SUM(C.SELECT_FOR_UPDATE_COUNT)
		          WHEN 'INSERT/UPDATE/DELETE'  THEN SUM(C.UPDATE_COUNT)
		          WHEN 'READ ONLY TRANSACTION' THEN SUM(C.READ_ONLY_TRANSACTION_COUNT)
	  	  	  WHEN 'UPDATE TRANSACTION'    THEN SUM(C.UPDATE_TRANSACTION_COUNT)
			  WHEN 'ROLLBACK'              THEN SUM(C.ROLLBACK_COUNT)
			  WHEN 'OTHERS'                THEN SUM(C.OTHERS_COUNT)
			  WHEN 'PREPARE'               THEN SUM(C.TOTAL_PREPARATION_COUNT)
		       END EXECUTIONS
		      ,CASE L.SQL_TYPE
		          WHEN 'SELECT'                THEN SUM(C.SELECT_TOTAL_EXECUTION_TIME) / 1000
			  WHEN 'SELECT FOR UPDATE'     THEN SUM(C.SELECT_FOR_UPDATE_TOTAL_EXECUTION_TIME) / 1000
			  WHEN 'INSERT/UPDATE/DELETE'  THEN SUM(C.UPDATE_TOTAL_EXECUTION_TIME) / 1000
			  WHEN 'READ ONLY TRANSACTION' THEN SUM(C.READ_ONLY_TRANSACTION_TOTAL_EXECUTION_TIME) / 1000
			  WHEN 'UPDATE TRANSACTION'    THEN SUM(C.UPDATE_TRANSACTION_TOTAL_EXECUTION_TIME) / 1000
			  WHEN 'ROLLBACK'              THEN SUM(C.ROLLBACK_TOTAL_EXECUTION_TIME) / 1000
			  WHEN 'OTHERS'                THEN SUM(C.OTHERS_TOTAL_EXECUTION_TIME) / 1000
			  WHEN 'PREPARE'               THEN SUM(C.TOTAL_PREPARATION_TIME) / 1000
		       END ELAPSED_MS
		      ,CASE L.SQL_TYPE
		          WHEN 'SELECT'                THEN MAP(SUM(C.SELECT_EXECUTION_COUNT), 0, 0, SUM(C.SELECT_TOTAL_EXECUTION_TIME) / 1000 / SUM(C.SELECT_EXECUTION_COUNT))
			  WHEN 'SELECT FOR UPDATE'     THEN MAP(SUM(C.SELECT_FOR_UPDATE_COUNT), 0, 0, SUM(C.SELECT_FOR_UPDATE_TOTAL_EXECUTION_TIME) / 1000 / SUM(C.SELECT_FOR_UPDATE_COUNT))
			  WHEN 'INSERT/UPDATE/DELETE'  THEN MAP(SUM(C.UPDATE_COUNT), 0, 0, SUM(C.UPDATE_TOTAL_EXECUTION_TIME) / 1000 / SUM(C.UPDATE_COUNT))
			  WHEN 'READ ONLY TRANSACTION' THEN MAP(SUM(C.READ_ONLY_TRANSACTION_COUNT), 0, 0, SUM(C.READ_ONLY_TRANSACTION_TOTAL_EXECUTION_TIME) / 1000 / SUM(C.READ_ONLY_TRANSACTION_COUNT))
			  WHEN 'UPDATE TRANSACTION'    THEN MAP(SUM(C.UPDATE_TRANSACTION_COUNT), 0, 0, SUM(C.UPDATE_TRANSACTION_TOTAL_EXECUTION_TIME) / 1000 / SUM(C.UPDATE_TRANSACTION_COUNT))
			  WHEN 'ROLLBACK'              THEN MAP(SUM(C.ROLLBACK_COUNT), 0, 0, SUM(C.ROLLBACK_TOTAL_EXECUTION_TIME) / 1000 / SUM(C.ROLLBACK_COUNT))
			  WHEN 'OTHERS'                THEN MAP(SUM(C.OTHERS_COUNT), 0, 0, SUM(C.OTHERS_TOTAL_EXECUTION_TIME) / 1000 / SUM(C.OTHERS_COUNT))
			  WHEN 'PREPARE'               THEN MAP(SUM(C.TOTAL_PREPARATION_COUNT), 0, 0, SUM(C.TOTAL_PREPARATION_TIME) / 1000 / SUM(C.TOTAL_PREPARATION_COUNT))
			  END ELA_PER_EXEC_MS
		      ,CASE L.SQL_TYPE
		          WHEN 'SELECT'                THEN 0
			  WHEN 'SELECT FOR UPDATE'     THEN MAP(SUM(C.SELECT_FOR_UPDATE_COUNT), 0, 0, SUM(C.SELECT_FOR_UPDATE_TOTAL_LOCK_WAIT_TIME) / 1000)
			  WHEN 'INSERT/UPDATE/DELETE'  THEN MAP(SUM(C.UPDATE_COUNT), 0, 0, SUM(C.UPDATE_TOTAL_LOCK_WAIT_TIME) / 1000)
			  WHEN 'READ ONLY TRANSACTION' THEN 0
			  WHEN 'UPDATE TRANSACTION'    THEN 0
			  WHEN 'ROLLBACK'              THEN 0
			  WHEN 'OTHERS'                THEN MAP(SUM(C.OTHERS_COUNT), 0, 0, SUM(C.OTHERS_TOTAL_LOCK_WAIT_TIME) / 1000)
			  WHEN 'PREPARE'               THEN 0 
		       END LOCK_WAIT_TIME_MS
		      ,CASE L.SQL_TYPE
		          WHEN 'SELECT'                THEN 0
			  WHEN 'SELECT FOR UPDATE'     THEN MAP(SUM(C.SELECT_FOR_UPDATE_COUNT), 0, 0, SUM(C.SELECT_FOR_UPDATE_TOTAL_LOCK_WAIT_TIME) / 1000 / SUM(C.SELECT_FOR_UPDATE_COUNT))
			  WHEN 'INSERT/UPDATE/DELETE'  THEN MAP(SUM(C.UPDATE_COUNT), 0, 0, SUM(C.UPDATE_TOTAL_LOCK_WAIT_TIME) / 1000 / SUM(C.UPDATE_COUNT))
			  WHEN 'READ ONLY TRANSACTION' THEN 0
			  WHEN 'UPDATE TRANSACTION'    THEN 0
			  WHEN 'ROLLBACK'              THEN 0
			  WHEN 'OTHERS'                THEN MAP(SUM(C.OTHERS_COUNT), 0, 0, SUM(C.OTHERS_TOTAL_LOCK_WAIT_TIME) / 1000 / SUM(C.OTHERS_COUNT))
			  WHEN 'PREPARE'               THEN 0 
		       END LOCK_PER_EXEC_MS
		      ,CASE L.SQL_TYPE
		          WHEN 'SELECT' THEN MAX(C.SELECT_MAX_EXECUTION_TIME) / 1000
			  WHEN 'SELECT FOR UPDATE' THEN MAX(C.SELECT_FOR_UPDATE_MAX_EXECUTION_TIME) / 1000
			  WHEN 'INSERT/UPDATE/DELETE' THEN MAX(C.UPDATE_MAX_EXECUTION_TIME) / 1000
			  WHEN 'READ ONLY TRANSACTION' THEN MAX(C.READ_ONLY_TRANSACTION_MAX_EXECUTION_TIME) / 1000
			  WHEN 'UPDATE TRANSACTION' THEN MAX(C.UPDATE_TRANSACTION_MAX_EXECUTION_TIME) / 1000
			  WHEN 'ROLLBACK' THEN MAX(C.ROLLBACK_MAX_EXECUTION_TIME) / 1000
			  WHEN 'OTHERS' THEN MAX(C.OTHERS_MAX_EXECUTION_TIME) / 1000
			  WHEN 'PREPARE' THEN MAX(C.MAX_PREPARATION_TIME) / 1000
		       END MAX_ELA_MS
		FROM SYS.M_SERVICES S,
	      (   SELECT 1 LINE_NO , 'SELECT' SQL_TYPE FROM DUMMY UNION ALL 
	        ( SELECT 2, 'SELECT FOR UPDATE' FROM DUMMY ) UNION ALL
	        ( SELECT 3, 'INSERT/UPDATE/DELETE' FROM DUMMY ) UNION ALL
	        ( SELECT 4, 'READ ONLY TRANSACTION' FROM DUMMY ) UNION ALL
	        ( SELECT 5, 'UPDATE TRANSACTION' FROM DUMMY ) UNION ALL
	        ( SELECT 6, 'ROLLBACK' FROM DUMMY ) UNION ALL
	        ( SELECT 7, 'OTHERS' FROM DUMMY ) UNION ALL
	        ( SELECT 8, 'PREPARE' FROM DUMMY ) 
              ) L,
	       SYS.M_CONNECTION_STATISTICS C
	    WHERE C.HOST = S.HOST
	      AND C.PORT = S.PORT
	    GROUP BY S.HOST, S.PORT, S.SERVICE_NAME, L.SQL_TYPE, L.LINE_NO);
	`
	schemaMemoryQuery = `
	SELECT host, schema_name, ROUND(SUM(memory_size_in_total)/1024/1024) schema_memory_used_mb
	FROM sys.m_cs_tables
	GROUP BY host, schema_name;
	`
	hostResourceUtilizationQuery = `
	SELECT host
              ,ROUND((used_physical_memory + free_physical_memory) / 1024 / 1024, 2) host_physical_mem_mb
              ,ROUND(used_physical_memory / 1024 / 1024, 2) host_resident_mem_mb
              ,ROUND(free_physical_memory / 1024 / 1024, 2) host_free_physical_mem_mb
              ,ROUND(free_swap_space / 1024 / 1024, 2) host_free_swap_mb
              ,ROUND(used_swap_space / 1024 / 1024, 2) host_used_swap_mb
              ,ROUND(allocation_limit / 1024 / 1024, 2) host_alloc_limit_mb
              ,ROUND(instance_total_memory_used_size / 1024 / 1024, 2) host_total_used_mem_mb
              ,ROUND(instance_total_memory_peak_used_size / 1024 / 1024, 2) host_total_peak_used_mem_mb
              ,ROUND(instance_total_memory_allocated_size / 1024 / 1024, 2) host_total_alloc_mem_mb
              ,ROUND(instance_code_size / 1024 / 1024, 2) host_code_size_mb
              ,ROUND(instance_shared_memory_allocated_size / 1024 / 1024, 2) host_shr_mem_alloc_mb
	      ,total_cpu_user_time
	      ,total_cpu_system_time
	      ,total_cpu_wio_time
	      ,total_cpu_idle_time
        FROM sys.m_host_resource_utilization;
	`
	hostDiskUsageQuery = `
	SELECT md.host
	      ,md.usage_type
	      ,md.path
	      ,md.filesystem_type
	      ,TO_DECIMAL(md.total_device_size / 1024 / 1024, 10, 2) total_device_size_mb
	      ,TO_DECIMAL(md.total_size / 1024 / 1024, 10, 2) total_size_mb
	      ,TO_DECIMAL(md.used_size / 1024 / 1024, 10, 2) total_used_size_mb
	      ,TO_DECIMAL(du.used_size / 1024 / 1024, 10, 2) used_size_mb
	      FROM sys.m_disk_usage du, sys.m_disks md
	      WHERE du.host = md.host AND du.usage_type = md.usage_type;
	`
	serviceReplicationQuery = `
	SELECT host
	      ,LPAD(port, 5) port
	      ,site_name
	      ,secondary_site_name
	      ,secondary_host
	      ,LPAD(secondary_port, 5) secondary_port
	      ,replication_mode
	      ,secondary_active_status
	      ,MAP(secondary_active_status, 'YES', 1, 0) secondary_active_status_code
	      ,replication_status
	      ,MAP(UPPER(replication_status),'ACTIVE',0,'ERROR',4,'SYNCING',2,'INITIALIZING',1,'UNKNOWN',3,99) replication_status_code
	      ,TO_DECIMAL(SECONDS_BETWEEN(SHIPPED_LOG_POSITION_TIME, LAST_LOG_POSITION_TIME), 10, 2) ship_delay_secs
	      ,TO_DECIMAL((LAST_LOG_POSITION - SHIPPED_LOG_POSITION) * 64 / 1024 / 1024, 10, 2) async_buff_used_mb
	      ,secondary_reconnect_count
	      ,secondary_failover_count
	FROM sys.m_service_replication;
	`
	currentAlertsQuery = `
	SELECT alert_host host
              ,LPAD(alert_port,5) port
              ,to_varchar(alert_rating) alert_rating
              ,alert_name
              ,count(*) count
        FROM _SYS_STATISTICS.STATISTICS_CURRENT_ALERTS
        GROUP BY alert_host, alert_port, alert_rating, alert_name
        ;
	`
	hostAgentCPUMetricsQuery = `
        SELECT 
	    HOST,
            MEASURED_ELEMENT_NAME CORE,
            SUM(MAP(CAPTION, 'User Time', TO_NUMBER(VALUE), 0)) USER_PCT,
            SUM(MAP(CAPTION, 'System Time', TO_NUMBER(VALUE), 0)) SYSTEM_PCT,
            SUM(MAP(CAPTION, 'Wait Time', TO_NUMBER(VALUE), 0)) WAITIO_PCT,
            SUM(MAP(CAPTION, 'Idle Time', 0, TO_NUMBER(VALUE))) BUSY_PCT,
            SUM(MAP(CAPTION, 'Idle Time', TO_NUMBER(VALUE), 0)) IDLE_PCT
        FROM sys.M_HOST_AGENT_METRICS
        WHERE MEASURED_ELEMENT_TYPE = 'Processor'
        GROUP BY HOST,
             MEASURED_ELEMENT_NAME;
	`
	hostAgentNetworkMetricsQuery = `
	SELECT host
              ,measured_element_name interface
              ,MAX(MAP(caption, 'Collision Rate', TO_NUMBER(value), 0)) collisions_per_sec
              ,MAX(MAP(caption, 'Receive Rate', TO_NUMBER(value), 0)) recv_kb_per_sec
              ,MAX(MAP(caption, 'Transmit Rate', TO_NUMBER(value), 0)) trans_kb_per_sec
              ,MAX(MAP(caption, 'Packet Receive Rate', TO_NUMBER(value), 0)) recv_pack_per_sec
              ,MAX(MAP(caption, 'Packet Transmit Rate', TO_NUMBER(value), 0)) trans_pack_per_sec
              ,MAX(MAP(caption, 'Receive Error Rate', TO_NUMBER(value), 0)) recv_err_per_sec
              ,MAX(MAP(caption, 'Transmit Error Rate', TO_NUMBER(value), 0)) trans_err_per_sec
       FROM sys.m_host_agent_metrics
       WHERE measured_element_type = 'NetworkPort' GROUP BY host, measured_element_name;
	`
	diskDataFilesQuery = `
	SELECT host,
              LPAD(port, 5) port,
              file_name,
              file_type,
              used_size / 1024 / 1024 used_size_mb,
              total_size / 1024 / 1024 total_size_mb,
              (total_size - used_size) / 1024 / 1024 available_size_mb,
              TO_DECIMAL(MAP(total_size,0,0,(1 - used_size / total_size) * 100),10,2) frag_pct
          FROM sys.m_volume_files
          WHERE file_type = 'DATA';
	`
	diskIOQuery = `
        SELECT host,
            disk,
            queue_length,
            srv_ms + wait_ms latency_ms,
            srv_ms,
            wait_ms,
            io_per_s,
            tp_kbps
        FROM(
          SELECT
            host,
            measured_element_name disk,
            MAX(MAP(caption, 'Queue Length', TO_NUMBER(value), 0)) queue_length,
            MAX(MAP(caption, 'Service Time', TO_NUMBER(value), 0)) srv_ms,
            MAX(MAP(caption, 'Wait Time', TO_NUMBER(value), 0)) wait_ms,
            MAX(MAP(caption, 'I/O Rate', TO_NUMBER(value), 0)) io_per_s,
            MAX( MAP(caption, 'Total Throughput', TO_NUMBER(value), 0)) tp_kbps
         FROM sys.m_host_agent_metrics
         WHERE measured_element_type = 'Disk'
         GROUP BY host,
            measured_element_name
        );
	`
	instanceWorkloadQuery = `
	SELECT D.HOST
              ,D.DATABASE_NAME
              ,W.EXECUTION_COUNT
              ,W.COMPILATION_COUNT
              ,UPDATE_TRANSACTION_COUNT
              ,COMMIT_COUNT
              ,ROLLBACK_COUNT
              ,CURRENT_EXECUTION_RATE
              ,PEAK_EXECUTION_RATE
              ,CURRENT_COMPILATION_RATE
              ,PEAK_COMPILATION_RATE
              ,CURRENT_UPDATE_TRANSACTION_RATE
              ,PEAK_UPDATE_TRANSACTION_RATE
              ,CURRENT_TRANSACTION_RATE
              ,PEAK_TRANSACTION_RATE
              ,CURRENT_COMMIT_RATE
              ,PEAK_COMMIT_RATE
              ,CURRENT_ROLLBACK_RATE
              ,PEAK_ROLLBACK_RATE
              ,CURRENT_MEMORY_USAGE_RATE
              ,PEAK_MEMORY_USAGE_RATE
        FROM SYS.M_DATABASE D, M_WORKLOAD W;
	`
	systemDBDatabasesQuery = `
	SELECT HOST
              ,PORT
              ,SQL_PORT
              ,DATABASE_NAME
              ,IS_DATABASE_LOCAL
	      ,MAP(ACTIVE_STATUS,'NO',0, 'YES', 1,'STARTING',2,'STOPPING',3,'UNKNOWN',4, 99) ACTIVE_STATUS
        FROM SYS_DATABASES.M_SERVICES
        WHERE SERVICE_NAME in ('nameserver','indexserver')
        ;
	`
	topHeapMemoryCategoriesQuery = `
        /*SELECT TOP %v <- this part is generated via code to custom Top elements*/
            HOST
            ,PORT
            ,COMPONENT
            ,CATEGORY
            ,ROUND(SUM(EXCLUSIVE_SIZE_IN_USE_MB),2) EXCLUSIVE_SIZE_IN_USE_MB
        FROM
        ( SELECT
             HOST
            ,PORT
            ,COMPONENT
            ,CASE
               WHEN CATEGORY LIKE '%/Pool/CalculationEngine%'                   THEN 'Pool/CalculationEngine'
               WHEN CATEGORY LIKE '%/Pool/JoinEvaluator'                        THEN 'Pool/JoinEvaluator'
               WHEN CATEGORY LIKE '%/Pool/RowEngine/QueryExecution/SearchAlloc' THEN 'Pool/RowEngine/QueryExecution/SearchAlloc'
               WHEN CATEGORY LIKE '%/Pool/RowEngine/Session'                    THEN 'Pool/RowEngine/Session'
               WHEN CATEGORY =    'AggregatedStatement'                         THEN 'AggregatedStatement'   /* for total_statement_memory_limit */
               WHEN CATEGORY LIKE 'Connection/%/Statement/%'                    THEN 'ConnectionStatement'
               WHEN CATEGORY LIKE 'Connection/______'                           THEN 'Connection'
               WHEN CATEGORY LIKE 'Connection/_______'                          THEN 'Connection'
               WHEN CATEGORY =    'Connection'                                  THEN 'Connection'
               WHEN CATEGORY LIKE 'HexPlan%'                                    THEN 'HexPlan'
               WHEN CATEGORY LIKE 'LVCContainerDir%'                            THEN 'LVCContainerDir'
               WHEN CATEGORY LIKE 'UnifiedTableComposite%'                      THEN 'UnifiedTableComposite'
               WHEN CATEGORY LIKE 'WorkloadCtx/%/Statement/%/IMPLICIT'          THEN 'WorkloadCtxStatement'
               WHEN CATEGORY LIKE 'WorkloadCtx%'                                THEN 'WorkloadCtx'
               WHEN CATEGORY LIKE 'XSApps%'                                     THEN 'XSApps'
               WHEN CATEGORY LIKE 'XSSessions%'                                 THEN 'XSSessions'
               WHEN CATEGORY =    '<unnamed allocator>'                         THEN 'unnamed_allocator'
               WHEN CATEGORY =    '/'                                           THEN 'Root'
               ELSE CATEGORY
             END CATEGORY
            ,SUM(EXCLUSIVE_SIZE_IN_USE)/1024/1024 EXCLUSIVE_SIZE_IN_USE_MB
          FROM M_HEAP_MEMORY
          WHERE EXCLUSIVE_SIZE_IN_USE > 1024
          GROUP BY HOST, PORT, COMPONENT, CATEGORY
        )
        GROUP BY HOST, PORT, COMPONENT, CATEGORY
        ORDER BY EXCLUSIVE_SIZE_IN_USE_MB DESC;
	`
	sdiRemoteSubscriptionsStadisticsQuery = `
        SELECT
        IFNULL(R.REMOTE_SOURCE_NAME,'') REMOTE_SOURCE_NAME,
        IFNULL(S.SCHEMA_NAME,'') SCHEMA_NAME,
        IFNULL(S.SUBSCRIPTION_NAME,'') SUBSCRIPTION_NAME,
        MAP(R.STATE, 'CREATED',0,'APPLY_CHANGE_DATA',1,'AUTO_CORRECT_CHANGE_DATA',2,'MAT_START_BEG_MARKER',3,'MAT_START_END_MARKER',4,'MAT_COMP_BEG_MARKER',5,'MAT_COMP_END_MARKER',6,99) STATE,
        S.RECEIVED_MESSAGE_COUNT RECEIVED,
        S.APPLIED_MESSAGE_COUNT APPLIED,
        S.REJECTED_MESSAGE_COUNT REJECTED,
        IFNULL(SECONDS_BETWEEN(S.LAST_MESSAGE_RECEIVED,CURRENT_TIMESTAMP),SECONDS_BETWEEN(R.LAST_PROCESSED_TRANSACTION_TIME,CURRENT_TIMESTAMP)) LAST_RECEIVE_SECS,
        IFNULL(SECONDS_BETWEEN(S.LAST_MESSAGE_APPLIED,CURRENT_TIMESTAMP),SECONDS_BETWEEN(R.LAST_PROCESSED_TRANSACTION_TIME,CURRENT_TIMESTAMP)) LAST_APPLY_TIME_SECS,
        CASE WHEN S.LAST_MESSAGE_APPLIED IS NULL THEN 0
                    WHEN S.LAST_MESSAGE_APPLIED > S.LAST_MESSAGE_RECEIVED THEN 0
                    WHEN S.LAST_MESSAGE_RECEIVED IS NULL THEN 0
                    ELSE SECONDS_BETWEEN(S.LAST_MESSAGE_APPLIED, S.LAST_MESSAGE_RECEIVED)
                END APPLY_DELAY_SECS,
        TO_DECIMAL(RECEIVER_LATENCY / 1000, 10, 0) RECEIVER_LATENCY_MS,
        TO_DECIMAL(APPLIER_LATENCY / 1000, 10, 0) APPLIER_LATENCY_MS
        FROM
        M_REMOTE_SUBSCRIPTION_STATISTICS S,
        M_REMOTE_SUBSCRIPTIONS R
        WHERE
        S.SUBSCRIPTION_NAME = R.SUBSCRIPTION_NAME AND
        S.SCHEMA_NAME = R.SCHEMA_NAME
        ORDER BY R.REMOTE_SOURCE_NAME,S.SCHEMA_NAME,S.SUBSCRIPTION_NAME
        ;
	`
	licenseUsageQuery = `
        SELECT  SYSTEM_ID,
                SYSTEM_NO,
                INSTALL_NO,
                PRODUCT_NAME,
                MAP(PERMANENT,TRUE,1,0) PERMANENT,
                MAP(VALID,TRUE,1,0) VALID,
                MAP(ENFORCED,TRUE,1,0) ENFORCED,
                MAP(LOCKED_DOWN,TRUE,1,0) LOCKED_DOWN,
                PRODUCT_LIMIT LICENSE_LIMIT_GB,
                PRODUCT_USAGE LICENSE_USAGE_GB,
                TO_DECIMAL(CASE WHEN PRODUCT_LIMIT = 0 THEN 0 ELSE PRODUCT_USAGE / PRODUCT_LIMIT * 100 END, 10, 2) USAGE_PCT
        FROM M_LICENSE WHERE PRODUCT_NAME LIKE 'SAP-HANA%';
	`
	serviceBufferCacheStatsQuery = `
        SELECT A.HOST,
            A.PORT,
            B.SERVICE_NAME,
            A.PLAN_CACHE_ENABLED is_plan_cache_enabled,
            MAP(A.STATISTICS_COLLECTION_ENABLED,TRUE,1,0) is_statistics_collection_enabled,
            ROUND(A.PLAN_CACHE_CAPACITY/1024/1024,2) plan_cache_capacity_mb,
            ROUND(A.CACHED_PLAN_SIZE/1204/1024,2) cached_plan_size_mb,
            ROUND(TO_DECIMAL(A.PLAN_CACHE_HIT_RATIO*100),2) plan_cache_hit_ratio,
            A.PLAN_CACHE_HIT_COUNT,
            A.EVICTED_PLAN_AVG_CACHE_TIME/1000 evicted_plan_avg_cache_time_ms,
            A.EVICTED_PLAN_COUNT,
            A.EVICTED_PLAN_PREPARATION_TIME/1000 evicted_plan_preparation_time_ms,
            ROUND(A.EVICTED_PLAN_SIZE/1204/1024,2) evicted_plan_size_mb,
            A.CACHED_PLAN_COUNT,
            A.CACHED_PLAN_PREPARATION_TIME/1000 cached_plan_preparation_time_ms
        FROM M_SQL_PLAN_CACHE_OVERVIEW A, M_SERVICES B
        WHERE A.PORT = B.PORT;
	`
	serviceThreadsQuery = `
        SELECT
                HOST
                ,PORT
                ,SERVICE_NAME
                ,MAP(WORKLOAD_CLASS_NAME,'','Unknown',WORKLOAD_CLASS_NAME) WORKLOAD_CLASS_NAME
                ,CASE
                WHEN THREAD_TYPE LIKE 'JobWrk%' THEN 'JobWorker'
                ELSE THREAD_TYPE
                END THREAD_TYPE
                ,CASE
                WHEN THREAD_METHOD LIKE 'GCJob%' THEN 'GCJob'
                WHEN THREAD_METHOD = '' THEN 'Unknown'
                ELSE THREAD_METHOD
                END THREAD_METHOD
                ,THREAD_STATE
                ,MAP(USER_NAME,'','Unknown',USER_NAME) USER_NAME
                ,MAP(APPLICATION_NAME,'','Unknown',APPLICATION_NAME) APPLICATION_NAME
                ,MAP(APPLICATION_USER_NAME,'','Unknown',APPLICATION_USER_NAME) APPLICATION_USER_NAME
                ,LOCK_WAIT_COMPONENT
                ,CASE
                WHEN LOCK_WAIT_NAME LIKE '%[%:%]@%:%' THEN
                        SUBSTR(LOCK_WAIT_NAME, LOCATE(LOCK_WAIT_NAME, '[') + 1, LOCATE(LOCK_WAIT_NAME, ':') - LOCATE(LOCK_WAIT_NAME, '[')) ||
                        SUBSTR(LOCK_WAIT_NAME, LOCATE(LOCK_WAIT_NAME, ':' || CHAR(32)) + 1)
                WHEN LOCK_WAIT_NAME = '' THEN
                        'Unknown'
                ELSE
                        LOCK_WAIT_NAME
                END LOCK_WAIT_NAME
                ,IS_ACTIVE
                ,MAP(CLIENT_IP,'','Unknown',CLIENT_IP) CLIENT_IP
                ,ROUND(AVG(DURATION),2) DURATION_MS_AVG
				,ROUND(AVG(CPU_TIME_SELF),2) CPU_TIME_MICRO_AVG
                ,COUNT(*) COUNT
        FROM M_SERVICE_THREADS
        GROUP BY HOST,PORT,SERVICE_NAME,WORKLOAD_CLASS_NAME,THREAD_TYPE,THREAD_METHOD,THREAD_STATE,USER_NAME,APPLICATION_NAME,APPLICATION_USER_NAME,LOCK_WAIT_COMPONENT,LOCK_WAIT_NAME,IS_ACTIVE,CLIENT_IP
        ORDER BY COUNT;
	`
	sdiRemoteSourceStatisticsQuery = `
		SELECT  RS.REMOTE_SOURCE_NAME
			,RS.SERVICE_NAME
			,RS.COMPONENT
			,RS.SUB_COMPONENT
			,RS.SUBSCRIPTION_NAME
			,RS.STATISTIC_NUMBER
			,CASE
			WHEN RS.STATISTIC_NUMBER = '10016' THEN 'last_received_message_secs'
			WHEN RS.STATISTIC_NUMBER = '10017' THEN 'last_distributed_message_secs'
			WHEN RS.STATISTIC_NUMBER = '10018' THEN 'last_applied_message_secs'
			WHEN RS.STATISTIC_NUMBER = '10019' THEN 'last_applied_message_for_subscription_secs'
			-------------
			WHEN RS.STATISTIC_NUMBER = '10001' THEN 'receiver_avg_record_process_time_microsecs'
			WHEN RS.STATISTIC_NUMBER = '10002' THEN 'distributor_avg_record_process_time_microsecs'
			WHEN RS.STATISTIC_NUMBER = '10004' THEN 'applier_avg_record_process_time_microsecs'
			-------------
			WHEN RS.STATISTIC_NUMBER = '10006' THEN 'receiver_meta_and_data_records_processed_count'
			WHEN RS.STATISTIC_NUMBER = '10007' THEN 'distributor_meta_and_data_records_processed_count'
			WHEN RS.STATISTIC_NUMBER = '10008' THEN 'applier_meta_and_data_records_processed_count'
			WHEN RS.STATISTIC_NUMBER = '10009' THEN 'applier_meta_and_data_records_processed_for_subscription_count'
			-------------
			WHEN RS.STATISTIC_NUMBER = '10011' THEN 'receiver_data_records_processed_count'
			WHEN RS.STATISTIC_NUMBER = '10012' THEN 'distributor_data_records_processed_count'
			WHEN RS.STATISTIC_NUMBER = '10013' THEN 'applier_data_records_processed_count'
			WHEN RS.STATISTIC_NUMBER = '10014' THEN 'applier_data_records_processed_for_subscription_count'
			-------------
			WHEN RS.STATISTIC_NUMBER = '10021' THEN 'applier_dml_insert_records_processed_for_subscription_count'
			WHEN RS.STATISTIC_NUMBER = '10023' THEN 'applier_dml_update_records_processed_for_subscription_count'
			-------------
			WHEN RS.STATISTIC_NUMBER = '10027' THEN 'last_updated_secs'
			-------------
			WHEN RS.STATISTIC_NUMBER = '40112' THEN 'num_invalidly_collect_trig_queue'
			WHEN RS.STATISTIC_NUMBER = '40113' THEN 'avg_time_collect_trig_queue'
			WHEN RS.STATISTIC_NUMBER = '40114' THEN 'num_trigger_records'
			WHEN RS.STATISTIC_NUMBER = '40115' THEN 'num_record_batches'
			WHEN RS.STATISTIC_NUMBER = '40116' THEN 'avg_records_per_trig_queue_scan'
			WHEN RS.STATISTIC_NUMBER = '40117' THEN 'avg_records_per_batch'
			WHEN RS.STATISTIC_NUMBER = '40118' THEN 'avg_scan_time_per_record_ms'
			WHEN RS.STATISTIC_NUMBER = '40119' THEN 'num_trans'
			WHEN RS.STATISTIC_NUMBER = '40120' THEN 'num_unscanned_records_in_trig_queue'
			WHEN RS.STATISTIC_NUMBER = '40121' THEN 'lastest_scanned_trans_time_in_trig_queue_secs'
			WHEN RS.STATISTIC_NUMBER = '40122' THEN 'lastest_trans_time_in_trig_queue_secs'
			WHEN RS.STATISTIC_NUMBER = '40124' THEN 'num_retrieved_results'
			WHEN RS.STATISTIC_NUMBER = '40125' THEN 'avg_time_retrieve_results_ms'
			WHEN RS.STATISTIC_NUMBER = '40126' THEN 'num_sent_rowsets'
			WHEN RS.STATISTIC_NUMBER = '40127' THEN 'num_sent_rows'
			WHEN RS.STATISTIC_NUMBER = '40128' THEN 'avg_rows_per_rowset'
			WHEN RS.STATISTIC_NUMBER = '40129' THEN 'avg_time_to_send_rowsets_ms'
			WHEN RS.STATISTIC_NUMBER = '40130' THEN 'latest_sent_time_in_applier_secs'
			WHEN RS.STATISTIC_NUMBER = '40143' THEN 'num_collect_trig_queue'
			WHEN RS.STATISTIC_NUMBER = '40143' THEN 'num_collect_trig_queue'
			WHEN RS.STATISTIC_NUMBER = '40144' THEN 'max_scan_time_per_record_ms'
			WHEN RS.STATISTIC_NUMBER = '40146' THEN 'min_scan_time_per_record_ms'
			ELSE RS.STATISTIC_NAME
			END STATISTIC_NAME
			,CASE
			WHEN RS.STATISTIC_NUMBER IN ('10016','10017','10018','10019','10027','40130') THEN SECONDS_BETWEEN(TO_TIMESTAMP(STATISTIC_VALUE), COLLECT_TIME)
			WHEN RS.STATISTIC_NUMBER IN ('40121','40122') THEN SECONDS_BETWEEN(UTCTOLOCAL(TO_TIMESTAMP(STATISTIC_VALUE)), COLLECT_TIME)
			WHEN RS.STATISTIC_NUMBER IN ('40116','40117','40118','40125','40129','40144','40146') THEN TO_INTEGER(TO_DECIMAL(RS.STATISTIC_VALUE)*1000)
			WHEN RS.STATISTIC_NUMBER IN ('40113','40116','40117','40128') THEN TO_INTEGER(TO_DECIMAL(RS.STATISTIC_VALUE))
			ELSE TO_INTEGER(RS.STATISTIC_VALUE)
			END STATISTIC_VALUE
		FROM
		(SELECT  REMOTE_SOURCE_NAME
			,SERVICE_NAME
			,COLLECT_TIME
			,COMPONENT
			,MAP(SUB_COMPONENT,'','no_data',SUB_COMPONENT) SUB_COMPONENT
			,MAP(SUBSCRIPTION_NAME,'','no_data',SUBSCRIPTION_NAME) SUBSCRIPTION_NAME
			,SUBSTR_BEFORE(STATISTIC_NAME,';') STATISTIC_NUMBER
			,SUBSTR_AFTER(STATISTIC_NAME,';') STATISTIC_NAME
			,STATISTIC_VALUE
		FROM M_REMOTE_SOURCE_STATISTICS
		WHERE (STATISTIC_NAME LIKE '%10016%'
			or STATISTIC_NAME LIKE '%10017%'
			or STATISTIC_NAME LIKE '%10018%'
			or STATISTIC_NAME LIKE '%10019%'
			or STATISTIC_NAME LIKE '%10001%'
			or STATISTIC_NAME LIKE '%10002%'
			or STATISTIC_NAME LIKE '%10004%'
			or STATISTIC_NAME LIKE '%10006%'
			or STATISTIC_NAME LIKE '%10007%'
			or STATISTIC_NAME LIKE '%10008%'
			or STATISTIC_NAME LIKE '%10009%'
			or STATISTIC_NAME LIKE '%10011%'
			or STATISTIC_NAME LIKE '%10012%'
			or STATISTIC_NAME LIKE '%10013%'
			or STATISTIC_NAME LIKE '%10014%'
			or STATISTIC_NAME LIKE '%10021%'
			or STATISTIC_NAME LIKE '%10023%'
			or STATISTIC_NAME LIKE '%10027%'
			or STATISTIC_NAME LIKE '%40112%'
			or STATISTIC_NAME LIKE '%40113%'
			or STATISTIC_NAME LIKE '%40114%'
			or STATISTIC_NAME LIKE '%40115%'
			or STATISTIC_NAME LIKE '%40116%'
			or STATISTIC_NAME LIKE '%40117%'
			or STATISTIC_NAME LIKE '%40118%'
			or STATISTIC_NAME LIKE '%40119%'
			or STATISTIC_NAME LIKE '%40120%'
			or STATISTIC_NAME LIKE '%40121%'
			or STATISTIC_NAME LIKE '%40122%'
			or STATISTIC_NAME LIKE '%40124%'
			or STATISTIC_NAME LIKE '%40125%'
			or STATISTIC_NAME LIKE '%40126%'
			or STATISTIC_NAME LIKE '%40127%'
			or STATISTIC_NAME LIKE '%40128%'
			or STATISTIC_NAME LIKE '%40129%'
			or STATISTIC_NAME LIKE '%40130%'
			or STATISTIC_NAME LIKE '%40143%'
			or STATISTIC_NAME LIKE '%40144%'
			or STATISTIC_NAME LIKE '%40146%'
			)
		) RS
		ORDER BY RS.REMOTE_SOURCE_NAME,RS.SUBSCRIPTION_NAME,RS.STATISTIC_NUMBER;
	`
)

func (m *HanaDB) getConnection(serv string) (*sql.DB, error) {
	db, err := sql.Open("hdb", serv)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (m *HanaDB) gatherServer(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {

	err := m.gatherDatabaseInstanceInfo(hi, db)
	if err != nil {
		return err
	}

	//m.Log.Infof("before m.GatherDatabaseDetails internal_schemas_used_mb: %v", hi.internal_schemas_memory_used_mb)
	if m.GatherDatabaseDetails {
		err = m.gatherDatabaseDetails(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherServiceMemory {
		err = m.gatherServiceMemory(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherServicePorts {
		err = m.gatherServicePorts(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherConnectionStats {
		err = m.gatherConnectionStats(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherSqlServiceStats {
		err = m.gatherSqlServiceStats(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherSchemaMemory {
		err = m.gatherSchemaMemory(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherHostResourceUtilization {
		err = m.gatherHostResourceUtilization(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherHostDiskUsage {
		err = m.gatherHostDiskUsage(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherServiceReplication {
		err = m.gatherServiceReplication(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherCurrentAlerts {
		err = m.gatherCurrentAlerts(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherHostAgentCPUMetrics {
		err = m.gatherHostAgentCPUMetrics(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherHostAgentNetworkMetrics {
		err = m.gatherHostAgentNetworkMetrics(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDiskDataFiles {
		err = m.gatherDiskDataFiles(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherDiskIO {
		err = m.gatherDiskIO(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherInstanceWorkload {
		err = m.gatherInstanceWorkload(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherSystemDBDatabases && hi.database_name == "SYSTEMDB" {
		err = m.gatherSystemDBDatabases(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherTopHeapMemoryCategories {
		err = m.gatherTopHeapMemoryCategories(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherSDIRemoteSubscriptionsStadistics {
		err = m.gatherSDIRemoteSubscriptionsStadistics(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherLicenseUsage {
		err = m.gatherLicenseUsage(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherServiceBufferCacheStats {
		err = m.gatherServiceBufferCacheStats(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherServiceThreads {
		err = m.gatherServiceThreads(hi, db, acc)
		if err != nil {
			return err
		}
	}

	if m.GatherSDIRemoteSourceStatistics {
		err = m.gatherGatherSDIRemoteSourceStatistics(hi, db, acc)
		if err != nil {
			return err
		}
	}

	return nil
}

// gatherDatabaseInstanceInfo can be used to collect database details
func (m *HanaDB) gatherDatabaseInstanceInfo(hi *HanaInstance, db *sql.DB) error {

	// run query
	rows, err := db.Query(databaseInstanceInfoQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		database_name   string
		host            string
		instance_id     string
		instance_number string
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 4 {
			if err := rows.Scan(&database_name, &host, &instance_id, &instance_number); err == nil {
				hi.instance_id = instance_id
				hi.instance_number = instance_number
				hi.host = host
				hi.database_name = database_name
			} else {
				return err
			}
		}
	}

	return nil
}

// gatherDatabaseInstanceInfo can be used to collect database details
func (m *HanaDB) getTenanInstances(db *sql.DB) ([]HanaInstance, error) {

	// run query
	var tenanList []HanaInstance
	//tenanList := make([string]interface{})
	rows, err := db.Query(tenanInstancesQuery)
	if err != nil {
		return tenanList, err
	}
	defer rows.Close()

	var (
		database_name string
		host          string
		sql_port      string
	)

	columns, err := rows.Columns()
	if err != nil {
		return tenanList, err
	}
	numColumns := len(columns)

	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 3 {
			if err := rows.Scan(&database_name, &host, &sql_port); err == nil {
				tenanList = append(tenanList, HanaInstance{url: fmt.Sprintf("%s:%s", host, sql_port),
					host:          host,
					database_name: database_name})
			} else {
				return tenanList, err
			}
		}
	}

	//m.Log.Infof("%s", tenanList)
	return tenanList, nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherDatabaseDetails(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(databaseDetailsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"up_time_secs":                    0,
		"colum_tables_total_used_mb":      0.0,
		"row_tables_fixedpart_used_mb":    0.0,
		"row_tables_variablepart_used_mb": 0.0,
		"row_tables_total_used_mb":        0.0,
	}

	var (
		host                            string
		instance_id                     string
		instance_number                 string
		distribuited                    string
		version                         string
		platform                        string
		database_name                   string
		uptime                          int
		colum_tables_total_used_mb      driver.Decimal
		row_tables_fixedpart_used_mb    driver.Decimal
		row_tables_variablepart_used_mb driver.Decimal
		row_tables_total_used_mb        driver.Decimal
		services_status                 int
		memory_status                   int
		cpu_status                      int
		disk_data_status                int
		disk_log_status                 int
		disk_trace_status               int
		alert_status                    int
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	//Collect only if GartherSchemaMemory already collect data
	/*m.Log.Infof("internal_schemas_used_mb: %v", hi.database_name)
	        m.Log.Infof("internal_schemas_used_mb: %v", hi.internal_schemas_memory_used_mb)
	        m.Log.Infof("user_schemas_memory_used_mb: %v", hi.user_schemas_memory_used_mb)
		if hi.internal_schemas_memory_used_mb > 0 {
		  fields["internal_schemas_memory_used_mb"] = hi.internal_schemas_memory_used_mb
		  fields["user_schemas_memory_used_mb"] = hi.user_schemas_memory_used_mb
	  	}*/
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 19 {
			if err := rows.Scan(&host,
				&instance_id,
				&instance_number,
				&distribuited,
				&version,
				&platform,
				&database_name,
				&uptime,
				&colum_tables_total_used_mb,
				&row_tables_fixedpart_used_mb,
				&row_tables_variablepart_used_mb,
				&row_tables_total_used_mb,
				&services_status,
				&memory_status,
				&cpu_status,
				&disk_data_status,
				&disk_log_status,
				&disk_trace_status,
				&alert_status); err == nil {

				tags["host"] = host
				tags["instance_id"] = instance_id
				tags["instance_number"] = instance_number
				tags["distribuited"] = distribuited
				tags["version"] = version
				tags["platform"] = platform
				tags["database_name"] = database_name
				fields["up_time_secs"] = uptime
				fields["colum_tables_total_used_mb"], _ = (*big.Rat)(&colum_tables_total_used_mb).Float64()
				fields["row_tables_fixedpart_used_mb"], _ = (*big.Rat)(&row_tables_fixedpart_used_mb).Float64()
				fields["row_tables_variablepart_used_mb"], _ = (*big.Rat)(&row_tables_variablepart_used_mb).Float64()
				fields["row_tables_total_used_mb"], _ = (*big.Rat)(&row_tables_total_used_mb).Float64()
				fields["services_status"] = services_status
				fields["memory_status"] = memory_status
				fields["cpu_status"] = cpu_status
				fields["disk_data_status"] = disk_data_status
				fields["disk_log_status"] = disk_log_status
				fields["disk_trace_status"] = disk_trace_status
				fields["alert_status"] = alert_status

				if hi.database_name != "SYSTEMDB" {
					tags["instance_type"] = "TENANDB"
				} else {
					tags["instance_type"] = "INTERNAL"
				}
				//m.Log.Infof("memory: %v", mem)

				acc.AddFields("hanadb_instance", fields, tags)
			} else {
				return err
			}
		}
	}

	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherServiceMemory(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(serviceMemoryQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"shm_alloc_mb":                      0.0,
		"shm_used_mb":                       0.0,
		"shm_used_pct":                      0.0,
		"heap_alloc_mb":                     0.0,
		"heap_used_mb":                      0.0,
		"heap_used_pct":                     0.0,
		"total_memory_used_mb":              0.0,
		"total_phys_mem_mb":                 0.0,
		"total_logical_mem_mb":              0.0,
		"code_size_mem_mb":                  0.0,
		"stack_size_mem_mb":                 0.0,
		"compactors_freeable_size_mem_mb":   0.0,
		"compactors_allocated_size_mem_mb":  0.0,
		"process_alloc_limit_mb":            0.0,
		"effective_proc_alloc_limit_mb":     0.0,
		"inclusive_peak_allocation_size_mb": 0.0,
		"heap_hex_mem_used_mb":              0.0,
	}

	var (
		host                              string
		port                              string
		service                           string
		shm_alloc_mb                      driver.Decimal
		shm_used_mb                       driver.Decimal
		shm_used_pct                      driver.Decimal
		heap_alloc_mb                     driver.Decimal
		heap_used_mb                      driver.Decimal
		heap_used_pct                     driver.Decimal
		total_memory_used_mb              driver.Decimal
		total_phys_mem_mb                 driver.Decimal
		total_logical_mem_mb              driver.Decimal
		code_size_mem_mb                  driver.Decimal
		stack_size_mem_mb                 driver.Decimal
		compactors_freeable_size_mem_mb   driver.Decimal
		compactors_allocated_size_mem_mb  driver.Decimal
		process_alloc_limit_mb            driver.Decimal
		effective_proc_alloc_limit_mb     driver.Decimal
		inclusive_peak_allocation_size_mb driver.Decimal
		heap_hex_mem_used_mb              driver.Decimal
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 20 {
			if err := rows.Scan(&host, &port, &service,
				&shm_alloc_mb, &shm_used_mb, &shm_used_pct, &heap_alloc_mb, &heap_used_mb,
				&heap_used_pct, &total_memory_used_mb, &total_phys_mem_mb, &total_logical_mem_mb, &code_size_mem_mb,
				&stack_size_mem_mb, &compactors_freeable_size_mem_mb, &compactors_allocated_size_mem_mb, &process_alloc_limit_mb, &effective_proc_alloc_limit_mb,
				&inclusive_peak_allocation_size_mb,
				&heap_hex_mem_used_mb); err == nil {

				tags["host"] = host
				tags["port"] = port
				tags["service"] = service
				fields["shared_memory_alloc_mb"], _ = (*big.Rat)(&shm_alloc_mb).Float64()
				fields["shared_memory_used_mb"], _ = (*big.Rat)(&shm_used_mb).Float64()
				fields["shared_memory_used_pct"], _ = (*big.Rat)(&shm_used_pct).Float64()
				fields["heap_alloc_mb"], _ = (*big.Rat)(&heap_alloc_mb).Float64()
				fields["heap_used_mb"], _ = (*big.Rat)(&heap_used_mb).Float64()
				fields["total_memory_used_mb"], _ = (*big.Rat)(&total_memory_used_mb).Float64()
				fields["total_phys_mem_mb"], _ = (*big.Rat)(&total_phys_mem_mb).Float64()
				fields["total_logical_mem_mb"], _ = (*big.Rat)(&total_logical_mem_mb).Float64()
				fields["code_size_mem_mb"], _ = (*big.Rat)(&code_size_mem_mb).Float64()
				fields["stack_size_mem_mb"], _ = (*big.Rat)(&stack_size_mem_mb).Float64()
				fields["compactors_freeable_size_mem_mb"], _ = (*big.Rat)(&compactors_freeable_size_mem_mb).Float64()
				fields["compactors_allocated_size_mem_mb"], _ = (*big.Rat)(&compactors_allocated_size_mem_mb).Float64()
				fields["process_alloc_limit_mb"], _ = (*big.Rat)(&process_alloc_limit_mb).Float64()
				fields["effective_proc_alloc_limit_mb"], _ = (*big.Rat)(&effective_proc_alloc_limit_mb).Float64()
				fields["inclusive_peak_allocation_size_mb"], _ = (*big.Rat)(&inclusive_peak_allocation_size_mb).Float64()
				fields["heap_hex_mem_used_mb"], _ = (*big.Rat)(&heap_hex_mem_used_mb).Float64()

				acc.AddFields("hanadb_service_memory", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherServicePorts(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(servicePortsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"up": 1,
	}

	var (
		service_name string
		port         string
		sql_port     string
		http_port    string
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["host"] = hi.host
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 4 {
			if err := rows.Scan(&service_name, &port, &sql_port, &http_port); err == nil {
				tags["service_name"] = service_name
				tags["port"] = port
				tags["sql_port"] = sql_port
				tags["http_port"] = http_port

				acc.AddFields("hanadb_service_ports", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherConnectionStats(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(connectionStatsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"total_connections": 0,
	}

	var (
		host              string
		port              string
		connection_type   string
		connection_status string
		total_connections int64
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 5 {
			if err := rows.Scan(&host,
				&port,
				&connection_type,
				&connection_status,
				&total_connections); err == nil {
				tags["host"] = host
				tags["port"] = port
				tags["connection_type"] = connection_type
				tags["connection_status"] = connection_status
				fields["total_connections"] = total_connections

				acc.AddFields("hanadb_connection_stats", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherSqlServiceStats(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(sqlServiceStatsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"executions":          0,
		"elapsed_ms":          0.0,
		"elapsed_per_exec_ms": 0.0,
		"lock_wait_time_ms":   0.0,
		"lock_per_exec_ms":    0.0,
		"max_elapsed_ms":      0.0,
	}

	var (
		host                string
		port                string
		service             string
		sql_type            string
		executions          int64
		elapsed_ms          driver.Decimal
		elapsed_per_exec_ms driver.Decimal
		lock_wait_time_ms   driver.Decimal
		lock_per_exec_ms    driver.Decimal
		max_elapsed_ms      driver.Decimal
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 10 {
			if err := rows.Scan(&host,
				&port,
				&service,
				&sql_type,
				&executions,
				&elapsed_ms,
				&elapsed_per_exec_ms,
				&lock_wait_time_ms,
				&lock_per_exec_ms,
				&max_elapsed_ms); err == nil {
				tags["host"] = host
				tags["port"] = port
				tags["service_name"] = service
				tags["sql_type"] = strings.Replace(sql_type, " ", "_", -1)
				fields["executions"] = executions
				fields["elapsed_ms"], _ = (*big.Rat)(&elapsed_ms).Float64()
				fields["elapsed_per_exec_ms"], _ = (*big.Rat)(&elapsed_per_exec_ms).Float64()
				fields["lock_wait_time_ms"], _ = (*big.Rat)(&lock_wait_time_ms).Float64()
				fields["lock_per_exec_ms"], _ = (*big.Rat)(&lock_per_exec_ms).Float64()
				fields["max_elapsed_ms"], _ = (*big.Rat)(&max_elapsed_ms).Float64()

				acc.AddFields("hanadb_sql_service_stats", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherHostResourceUtilization(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(hostResourceUtilizationQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"physical_mem_mb":        0.0,
		"resident_mem_mb":        0.0,
		"free_physical_mem_mb":   0.0,
		"free_swap_mb":           0.0,
		"used_swap_mb":           0.0,
		"alloc_limit_mb":         0.0,
		"total_used_mem_mb":      0.0,
		"total_peak_used_mem_mb": 0.0,
		"total_alloc_mem_mb":     0.0,
		"code_size_mb":           0.0,
		"shr_mem_alloc_mb":       0.0,
	}

	var (
		host                        string
		host_physical_mem_mb        driver.Decimal
		host_resident_mem_mb        driver.Decimal
		host_free_physical_mem_mb   driver.Decimal
		host_free_swap_mb           driver.Decimal
		host_used_swap_mb           driver.Decimal
		host_alloc_limit_mb         driver.Decimal
		host_total_used_mem_mb      driver.Decimal
		host_total_peak_used_mem_mb driver.Decimal
		host_total_alloc_mem_mb     driver.Decimal
		host_code_size_mb           driver.Decimal
		host_shr_mem_alloc_mb       driver.Decimal
		total_cpu_user_time         int64
		total_cpu_system_time       int64
		total_cpu_wio_time          int64
		total_cpu_idle_time         int64
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 16 {
			if err := rows.Scan(&host,
				&host_physical_mem_mb,
				&host_resident_mem_mb,
				&host_free_physical_mem_mb,
				&host_free_swap_mb,
				&host_used_swap_mb,
				&host_alloc_limit_mb,
				&host_total_used_mem_mb,
				&host_total_peak_used_mem_mb,
				&host_total_alloc_mem_mb,
				&host_code_size_mb,
				&host_shr_mem_alloc_mb,
				&total_cpu_user_time,
				&total_cpu_system_time,
				&total_cpu_wio_time,
				&total_cpu_idle_time); err == nil {
				tags["host"] = host
				fields["physical_mem_mb"], _ = (*big.Rat)(&host_physical_mem_mb).Float64()
				fields["resident_mem_mb"], _ = (*big.Rat)(&host_resident_mem_mb).Float64()
				fields["free_physical_mem_mb"], _ = (*big.Rat)(&host_free_physical_mem_mb).Float64()
				fields["free_swap_mb"], _ = (*big.Rat)(&host_free_swap_mb).Float64()
				fields["used_swap_mb"], _ = (*big.Rat)(&host_used_swap_mb).Float64()
				fields["alloc_limit_mb"], _ = (*big.Rat)(&host_alloc_limit_mb).Float64()
				fields["total_used_mem_mb"], _ = (*big.Rat)(&host_total_used_mem_mb).Float64()
				fields["total_peak_used_mem_mb"], _ = (*big.Rat)(&host_total_peak_used_mem_mb).Float64()
				fields["total_alloc_mem_mb"], _ = (*big.Rat)(&host_total_alloc_mem_mb).Float64()
				fields["code_size_mb"], _ = (*big.Rat)(&host_code_size_mb).Float64()
				fields["shr_mem_alloc_mb"], _ = (*big.Rat)(&host_shr_mem_alloc_mb).Float64()
				fields["shr_mem_alloc_mb"], _ = (*big.Rat)(&host_shr_mem_alloc_mb).Float64()
				fields["total_cpu_user_time"] = total_cpu_user_time
				fields["total_cpu_system_time"] = total_cpu_system_time
				fields["total_cpu_wio_time"] = total_cpu_wio_time
				fields["total_cpu_idle_time"] = total_cpu_idle_time

				acc.AddFields("hanadb_host_resource_utilization", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherSchemaMemory(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(schemaMemoryQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"schema_memory_used_mb": 0.0,
	}

	var (
		host                  string
		schema_name           string
		schema_memory_used_mb driver.Decimal
	)
	internal_schemas_used_mb := 0.0
	user_schemas_used_mb := 0.0

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 3 {
			if err := rows.Scan(&host, &schema_name, &schema_memory_used_mb); err == nil {
				memory, _ := (*big.Rat)(&schema_memory_used_mb).Float64()
				isInternalSchema := false
				for _, prefix := range m.InternalSchemaPrefix {
					if len(schema_name) >= len(prefix) && schema_name[:len(prefix)] == prefix {
						isInternalSchema = true
						internal_schemas_used_mb += memory
						break
					}
				}
				if (!isInternalSchema) && m.InternalSchemaSuffix != nil {
					for _, suffix := range m.InternalSchemaSuffix {
						if len(schema_name) >= len(suffix) && schema_name[len(schema_name)-len(suffix):len(schema_name)] == suffix {
							isInternalSchema = true
							internal_schemas_used_mb += memory
							break
						}
					}
				}
				if !isInternalSchema {
					user_schemas_used_mb += memory
					tags["host"] = host
					tags["schema_name"] = schema_name
					tags["schema_type"] = "USER_SCHEMA"
					fields["schema_memory_used_mb"] = memory

					if memory > 0 {
						acc.AddFields("hanadb_schema_memory", fields, tags)
					}
				} else {
					if m.CollectInternalSchemaMemory {
						tags["host"] = host
						tags["schema_name"] = schema_name
						tags["schema_type"] = "INTERNAL_SCHEMA"
						fields["schema_memory_used_mb"] = memory

						if memory > 0 {
							acc.AddFields("hanadb_schema_memory", fields, tags)
						}
					}
				}

				//m.Log.Infof("memory: %v", mem)

			} else {
				return err
			}
		}
	}

	tags["host"] = hi.host
	tags["schema_name"] = "SUMMARY"
	tags["schema_type"] = "SUMMARY"
	fields["schema_memory_used_mb"] = user_schemas_used_mb
	fields["internal_schema_memory_used_mb"] = internal_schemas_used_mb
	fields["total_schema_memory_used_mb"] = internal_schemas_used_mb + user_schemas_used_mb
	acc.AddFields("hanadb_schema_memory_summary", fields, tags)
	/*if internal_schemas_used_mb > 0 {
			hi.internal_schemas_memory_used_mb =  internal_schemas_used_mb
	                m.Log.Infof("internal_schemas_used_mb: %v", hi.internal_schemas_memory_used_mb)
		}

		if user_schemas_used_mb > 0 {
			hi.user_schemas_memory_used_mb =  user_schemas_used_mb
	                //m.Log.Infof("user_schemas_used_mb: %v", hi.user_schemas_memory_used_mb)
		}*/

	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherHostDiskUsage(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(hostDiskUsageQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"total_device_size_mb": 0.0,
		"total_size_mb":        0.0,
		"total_used_size_mb":   0.0,
		"used_size_mb":         0.0,
	}

	var (
		host                 string
		usage_type           string
		path                 string
		filesystem_type      string
		total_device_size_mb driver.Decimal
		total_size_mb        driver.Decimal
		total_used_size_mb   driver.Decimal
		used_size_mb         driver.Decimal
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 8 {
			if err := rows.Scan(&host,
				&usage_type,
				&path,
				&filesystem_type,
				&total_device_size_mb,
				&total_size_mb,
				&total_used_size_mb,
				&used_size_mb); err == nil {
				tags["host"] = host
				tags["usage_type"] = usage_type
				tags["path"] = path
				tags["filesystem_type"] = filesystem_type
				fields["total_device_size_mb"], _ = (*big.Rat)(&total_device_size_mb).Float64()
				fields["total_size_mb"], _ = (*big.Rat)(&total_size_mb).Float64()
				fields["total_used_size_mb"], _ = (*big.Rat)(&total_used_size_mb).Float64()
				fields["used_size_mb"], _ = (*big.Rat)(&used_size_mb).Float64()

				acc.AddFields("hanadb_host_disk_usage", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherServiceReplication(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(serviceReplicationQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"secondary_active_status_code": 0,
		"replication_status_code":      0,
		"ship_delay_secs":              0.0,
		"async_buff_used_mb":           0.0,
		"secondary_reconnect_count":    0,
		"secondary_failover_count":     0,
	}

	var (
		host                         string
		port                         string
		site_name                    string
		secondary_site_name          string
		secondary_host               string
		secondary_port               string
		replication_mode             string
		secondary_active_status      string
		secondary_active_status_code int64
		replication_status           string
		replication_status_code      int64
		ship_delay_secs              driver.Decimal
		async_buff_used_mb           driver.Decimal
		secondary_reconnect_count    int64
		secondary_failover_count     int64
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 15 {
			if err := rows.Scan(&host,
				&port,
				&site_name,
				&secondary_site_name,
				&secondary_host,
				&secondary_port,
				&replication_mode,
				&secondary_active_status,
				&secondary_active_status_code,
				&replication_status,
				&replication_status_code,
				&ship_delay_secs,
				&async_buff_used_mb,
				&secondary_reconnect_count,
				&secondary_failover_count); err == nil {
				tags["host"] = host
				tags["port"] = port
				tags["site_name"] = site_name
				tags["secondary_site_name"] = secondary_site_name
				tags["secondary_host"] = secondary_host
				tags["secondary_port"] = secondary_port
				tags["replication_mode"] = replication_mode
				tags["secondary_active_status"] = secondary_active_status
				tags["replication_status"] = replication_status
				fields["secondary_active_status_code"] = secondary_active_status_code
				fields["replication_status_code"] = replication_status_code
				fields["ship_delay_secs"], _ = (*big.Rat)(&ship_delay_secs).Float64()
				fields["async_buff_used_mb"], _ = (*big.Rat)(&async_buff_used_mb).Float64()
				fields["secondary_reconnect_count"] = secondary_reconnect_count
				fields["secondary_failover_count"] = secondary_failover_count

				acc.AddFields("hanadb_service_replication", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherCurrentAlerts(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(currentAlertsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"count": 0,
	}

	var (
		host         string
		port         string
		alert_rating string
		alert_name   string
		count        int64
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 5 {
			if err := rows.Scan(&host,
				&port,
				&alert_rating,
				&alert_name,
				&count); err == nil {
				tags["host"] = host
				tags["port"] = port
				tags["alert_rating"] = alert_rating
				tags["alert_name"] = alert_name
				fields["count"] = count

				acc.AddFields("hanadb_current_alerts", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherHostAgentCPUMetrics(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(hostAgentCPUMetricsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"user_pct":   0.0,
		"system_pct": 0.0,
		"waitio_pct": 0.0,
		"busy_pct":   0.0,
		"idle_pct":   0.0,
	}

	var (
		host       string
		core       string
		user_pct   driver.Decimal
		system_pct driver.Decimal
		waitio_pct driver.Decimal
		busy_pct   driver.Decimal
		idle_pct   driver.Decimal
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 7 {
			if err := rows.Scan(&host,
				&core,
				&user_pct,
				&system_pct,
				&waitio_pct,
				&busy_pct,
				&idle_pct); err == nil {
				tags["host"] = host
				tags["core"] = core
				fields["user_pct"], _ = (*big.Rat)(&user_pct).Float64()
				fields["system_pct"], _ = (*big.Rat)(&system_pct).Float64()
				fields["waitio_pct"], _ = (*big.Rat)(&waitio_pct).Float64()
				fields["busy_pct"], _ = (*big.Rat)(&busy_pct).Float64()
				fields["idle_pct"], _ = (*big.Rat)(&idle_pct).Float64()

				acc.AddFields("hanadb_host_agent_cpu", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherHostAgentNetworkMetrics(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(hostAgentNetworkMetricsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"collisions_per_sec": 0.0,
		"recv_kb_per_sec":    0.0,
		"trans_kb_per_sec":   0.0,
		"recv_pack_per_sec":  0.0,
		"trans_pack_per_sec": 0.0,
		"recv_err_per_sec":   0.0,
		"trans_err_per_sec":  0.0,
	}

	var (
		host               string
		net_interface      string
		collisions_per_sec driver.Decimal
		recv_kb_per_sec    driver.Decimal
		trans_kb_per_sec   driver.Decimal
		recv_pack_per_sec  driver.Decimal
		trans_pack_per_sec driver.Decimal
		recv_err_per_sec   driver.Decimal
		trans_err_per_sec  driver.Decimal
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 9 {
			if err := rows.Scan(&host,
				&net_interface,
				&collisions_per_sec,
				&recv_kb_per_sec,
				&trans_kb_per_sec,
				&recv_pack_per_sec,
				&trans_pack_per_sec,
				&recv_err_per_sec,
				&trans_err_per_sec); err == nil {
				tags["host"] = host
				tags["interface"] = net_interface
				fields["collisions_per_sec"], _ = (*big.Rat)(&collisions_per_sec).Float64()
				fields["recv_kb_per_sec"], _ = (*big.Rat)(&recv_kb_per_sec).Float64()
				fields["trans_kb_per_sec"], _ = (*big.Rat)(&trans_kb_per_sec).Float64()
				fields["recv_pack_per_sec"], _ = (*big.Rat)(&recv_pack_per_sec).Float64()
				fields["trans_pack_per_sec"], _ = (*big.Rat)(&trans_pack_per_sec).Float64()
				fields["recv_err_per_sec"], _ = (*big.Rat)(&recv_err_per_sec).Float64()
				fields["trans_err_per_sec"], _ = (*big.Rat)(&trans_err_per_sec).Float64()

				acc.AddFields("hanadb_host_agent_network", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

func (m *HanaDB) gatherDiskDataFiles(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(diskDataFilesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"used_size_mb":      0.0,
		"total_size_mb":     0.0,
		"available_size_mb": 0.0,
		"fragment_pct":      0.0,
	}

	var (
		host              string
		port              string
		file_name         string
		file_type         string
		used_size_mb      driver.Decimal
		total_size_mb     driver.Decimal
		available_size_mb driver.Decimal
		frag_pct          driver.Decimal
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 8 {
			if err := rows.Scan(&host,
				&port,
				&file_name,
				&file_type,
				&used_size_mb,
				&total_size_mb,
				&available_size_mb,
				&frag_pct); err == nil {
				tags["host"] = host
				tags["port"] = port
				tags["file_name"] = file_name
				tags["file_type"] = file_type
				fields["used_size_mb"], _ = (*big.Rat)(&used_size_mb).Float64()
				fields["total_size_mb"], _ = (*big.Rat)(&total_size_mb).Float64()
				fields["available_size_mb"], _ = (*big.Rat)(&available_size_mb).Float64()
				fields["frag_pct"], _ = (*big.Rat)(&frag_pct).Float64()

				acc.AddFields("hanadb_disk_data_files", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherDiskIO(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(diskIOQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"queue_length":           0.0,
		"latency_ms":             0.0,
		"service_time_ms":        0.0,
		"io_wait_time_ms":        0.0,
		"io_requests_per_second": 0.0,
		"total_throughput_kbps":  0.0,
	}

	var (
		host                   string
		disk                   string
		queue_length           driver.Decimal
		latency_ms             driver.Decimal
		service_time_ms        driver.Decimal
		io_wait_time_ms        driver.Decimal
		io_requests_per_second driver.Decimal
		tp_kbps                driver.Decimal
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 8 {
			if err := rows.Scan(&host,
				&disk,
				&queue_length,
				&latency_ms,
				&service_time_ms,
				&io_wait_time_ms,
				&io_requests_per_second,
				&tp_kbps); err == nil {
				tags["host"] = host
				tags["disk"] = disk
				fields["queue_length"], _ = (*big.Rat)(&queue_length).Float64()
				fields["latency_ms"], _ = (*big.Rat)(&latency_ms).Float64()
				fields["service_time_ms"], _ = (*big.Rat)(&service_time_ms).Float64()
				fields["io_wait_time_ms"], _ = (*big.Rat)(&io_wait_time_ms).Float64()
				fields["io_requests_per_second"], _ = (*big.Rat)(&io_requests_per_second).Float64()
				fields["total_throughput_kbps"], _ = (*big.Rat)(&tp_kbps).Float64()

				acc.AddFields("hanadb_disk_io", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherInstanceWorkload(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(instanceWorkloadQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"execution_count":                 0,
		"compilation_count":               0,
		"update_transaction_count":        0,
		"commit_count":                    0,
		"rollback_count":                  0,
		"current_execution_rate":          0,
		"peak_execution_rate":             0.0,
		"current_compilation_rate":        0,
		"peak_compilation_rate":           0.0,
		"current_update_transaction_rate": 0,
		"peak_update_transaction_rate":    0.0,
		"current_transaction_rate":        0,
		"peak_transaction_rate":           0.0,
		"current_commit_rate":             0,
		"peak_commit_rate":                0.0,
		"current_rollback_rate":           0,
		"peak_rollback_rate":              0.0,
		"current_memory_usage_rate":       0,
		"peak_memory_usage_rate":          0.0,
	}

	var (
		host                            string
		database_name                   string
		execution_count                 int64
		compilation_count               int64
		update_transaction_count        int64
		commit_count                    int64
		rollback_count                  int64
		current_execution_rate          float64
		peak_execution_rate             float64
		current_compilation_rate        float64
		peak_compilation_rate           float64
		current_update_transaction_rate float64
		peak_update_transaction_rate    float64
		current_transaction_rate        float64
		peak_transaction_rate           float64
		current_commit_rate             float64
		peak_commit_rate                float64
		current_rollback_rate           float64
		peak_rollback_rate              float64
		current_memory_usage_rate       float64
		peak_memory_usage_rate          float64
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 21 {
			if err := rows.Scan(&host,
				&database_name,
				&execution_count,
				&compilation_count,
				&update_transaction_count,
				&commit_count,
				&rollback_count,
				&current_execution_rate,
				&peak_execution_rate,
				&current_compilation_rate,
				&peak_compilation_rate,
				&current_update_transaction_rate,
				&peak_update_transaction_rate,
				&current_transaction_rate,
				&peak_transaction_rate,
				&current_commit_rate,
				&peak_commit_rate,
				&current_rollback_rate,
				&peak_rollback_rate,
				&current_memory_usage_rate,
				&peak_memory_usage_rate); err == nil {
				tags["host"] = host
				tags["database_name"] = database_name
				fields["execution_count"] = execution_count
				fields["compilation_count"] = compilation_count
				fields["update_transaction_count"] = update_transaction_count
				fields["commit_count"] = commit_count
				fields["rollback_count"] = rollback_count
				fields["current_execution_rate"] = current_execution_rate
				fields["peak_execution_rate"] = peak_execution_rate
				fields["current_compilation_rate"] = current_compilation_rate
				fields["peak_compilation_rate"] = peak_compilation_rate
				fields["current_update_transaction_rate"] = current_update_transaction_rate
				fields["peak_update_transaction_rate"] = peak_update_transaction_rate
				fields["current_transaction_rate"] = current_transaction_rate
				fields["peak_transaction_rate"] = peak_transaction_rate
				fields["current_commit_rate"] = current_commit_rate
				fields["peak_commit_rate"] = peak_commit_rate
				fields["current_rollback_rate"] = current_rollback_rate
				fields["peak_rollback_rate"] = peak_rollback_rate
				fields["current_memory_usage_rate"] = current_memory_usage_rate
				fields["peak_memory_usage_rate"] = peak_memory_usage_rate

				acc.AddFields("hanadb_instance_workload", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherSystemDBDatabases(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(systemDBDatabasesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"active_status": 0,
	}

	var (
		host              string
		port              string
		sql_port          string
		database_name     string
		is_database_local string
		active_status     int64
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["host"] = hi.host
	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 6 {
			if err := rows.Scan(&host,
				&port,
				&sql_port,
				&database_name,
				&is_database_local,
				&active_status); err == nil {
				tags["db_host"] = host
				tags["db_port"] = port
				tags["db_sqlport"] = sql_port
				tags["db_name"] = database_name
				tags["is_database_local"] = is_database_local
				fields["active_status"] = active_status

				acc.AddFields("hanadb_systemdb_databases", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherTopHeapMemoryCategories(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	querySQL := fmt.Sprintf("SELECT TOP %v \n%s", m.TopHeapMemoryCategories, topHeapMemoryCategoriesQuery)
	//rows, err := db.Query(fmt.Sprintf(topHeapMemoryCategoriesQuery,m.TopHeapMemoryCategories))
	rows, err := db.Query(querySQL)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"exclusive_size_in_use_mb": 0.0,
	}

	var (
		host                     string
		port                     string
		component                string
		category                 string
		exclusive_size_in_use_mb driver.Decimal
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 5 {
			if err := rows.Scan(&host,
				&port,
				&component,
				&category,
				&exclusive_size_in_use_mb); err == nil {
				tags["host"] = host
				tags["port"] = port
				tags["component"] = component
				tags["category"] = category
				fields["exclusive_size_in_use_mb"], _ = (*big.Rat)(&exclusive_size_in_use_mb).Float64()

				acc.AddFields("hanadb_top_heap_memory_categories", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherSDIRemoteSubscriptionsStadistics collect SDi subcription stats
func (m *HanaDB) gatherSDIRemoteSubscriptionsStadistics(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(sdiRemoteSubscriptionsStadisticsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"state":                       0,
		"received_msg_count":          0,
		"applied_msg_count":           0,
		"rejected_msg_count":          0,
		"last_msg_received_secs":      0,
		"last_msg_applied_secs":       0,
		"last_msg_applied_delay_secs": 0,
		"receiver_latency_ms":         0.0,
		"applier_latency_ms":          0.0,
	}

	var (
		remote_source_name          string
		schema_name                 string
		subscription_name           string
		state                       int64
		received_msg_count          int64
		applied_msg_count           int64
		rejected_msg_count          int64
		last_msg_received_secs      int64
		last_msg_applied_secs       int64
		last_msg_applied_delay_secs int64
		receiver_latency_ms         driver.Decimal
		applier_latency_ms          driver.Decimal
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["host"] = hi.host
	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 12 {
			if err := rows.Scan(&remote_source_name,
				&schema_name,
				&subscription_name,
				&state,
				&received_msg_count,
				&applied_msg_count,
				&rejected_msg_count,
				&last_msg_received_secs,
				&last_msg_applied_secs,
				&last_msg_applied_delay_secs,
				&receiver_latency_ms,
				&applier_latency_ms); err == nil {
				tags["remote_source_name"] = remote_source_name
				tags["schema_name"] = schema_name
				tags["subscription_name"] = subscription_name
				fields["state"] = state
				fields["received_msg_count"] = received_msg_count
				fields["applied_msg_count"] = applied_msg_count
				fields["rejected_msg_count"] = rejected_msg_count
				fields["last_msg_received_secs"] = last_msg_received_secs
				fields["last_msg_applied_secs"] = last_msg_applied_secs
				fields["last_msg_applied_delay_secs"] = last_msg_applied_delay_secs
				fields["receiver_latency_ms"], _ = (*big.Rat)(&receiver_latency_ms).Float64()
				fields["applier_latency_ms"], _ = (*big.Rat)(&applier_latency_ms).Float64()

				acc.AddFields("hanadb_sdi_remote_subcriptions_stats", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherLicenseUsage(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(licenseUsageQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"is_permament":     0,
		"is_valid":         0,
		"is_enforced":      0,
		"is_locked_down":   0,
		"license_limit_gb": 0,
		"license_usage_gb": 0,
		"usage_pct":        0.0,
	}

	var (
		instance_id      string
		system_number    string
		install_number   string
		product_name     string
		is_permament     int64
		is_valid         int64
		is_enforced      int64
		is_locked_down   int64
		license_limit_gb int64
		license_usage_gb int64
		usage_pct        driver.Decimal
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["host"] = hi.host
	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 11 {
			if err := rows.Scan(&instance_id,
				&system_number,
				&install_number,
				&product_name,
				&is_permament,
				&is_valid,
				&is_enforced,
				&is_locked_down,
				&license_limit_gb,
				&license_usage_gb,
				&usage_pct); err == nil {
				tags["instance_id"] = instance_id
				tags["system_number"] = system_number
				tags["install_number"] = install_number
				tags["product_name"] = product_name
				fields["is_permament"] = is_permament
				fields["is_valid"] = is_valid
				fields["is_enforced"] = is_enforced
				fields["is_locked_down"] = is_locked_down
				fields["license_limit_gb"] = license_limit_gb
				fields["license_usage_gb"] = license_usage_gb
				fields["usage_pct"], _ = (*big.Rat)(&usage_pct).Float64()

				acc.AddFields("hanadb_license_usage", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherDatabaseDetails can be used to collect database details
func (m *HanaDB) gatherServiceBufferCacheStats(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(serviceBufferCacheStatsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"is_plan_cache_enabled":            0,
		"is_statistics_collection_enabled": 0,
		"plan_cache_capacity_mb":           0.0,
		"cached_plan_size_mb":              0.0,
		"plan_cache_hit_ratio":             0,
		"plan_cache_hit_count":             0,
		"evicted_plan_avg_cache_time_ms":   0.0,
		"evicted_plan_count":               0,
		"evicted_plan_preparation_time_ms": 0.0,
		"evicted_plan_size_mb":             0.0,
		"cached_plan_count":                0,
		"cached_plan_preparation_time_ms":  0.0,
	}

	var (
		host                             string
		port                             string
		service_name                     string
		is_plan_cache_enabled            int64
		is_statistics_collection_enabled int64
		plan_cache_capacity_mb           driver.Decimal
		cached_plan_size_mb              driver.Decimal
		plan_cache_hit_ratio_pct         driver.Decimal
		plan_cache_hit_count             int64
		evicted_plan_avg_cache_time_ms   driver.Decimal
		evicted_plan_count               int64
		evicted_plan_preparation_time_ms driver.Decimal
		evicted_plan_size_mb             driver.Decimal
		cached_plan_count                int64
		cached_plan_preparation_time_ms  driver.Decimal
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["host"] = hi.host
	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 15 {
			if err := rows.Scan(&host,
				&port,
				&service_name,
				&is_plan_cache_enabled,
				&is_statistics_collection_enabled,
				&plan_cache_capacity_mb,
				&cached_plan_size_mb,
				&plan_cache_hit_ratio_pct,
				&plan_cache_hit_count,
				&evicted_plan_avg_cache_time_ms,
				&evicted_plan_count,
				&evicted_plan_preparation_time_ms,
				&evicted_plan_size_mb,
				&cached_plan_count,
				&cached_plan_preparation_time_ms); err == nil {
				tags["host"] = host
				tags["port"] = port
				tags["service_name"] = service_name
				fields["is_plan_cache_enabled"] = is_plan_cache_enabled
				fields["is_statistics_collection_enabled"] = is_statistics_collection_enabled
				fields["plan_cache_capacity_mb"], _ = (*big.Rat)(&plan_cache_capacity_mb).Float64()
				fields["cached_plan_size_mb"], _ = (*big.Rat)(&cached_plan_size_mb).Float64()
				fields["plan_cache_hit_ratio_pct"], _ = (*big.Rat)(&plan_cache_hit_ratio_pct).Float64()
				fields["plan_cache_hit_count"] = plan_cache_hit_count
				fields["evicted_plan_avg_cache_time_ms"], _ = (*big.Rat)(&evicted_plan_avg_cache_time_ms).Float64()
				fields["evicted_plan_count"] = evicted_plan_count
				fields["evicted_plan_preparation_time_ms"], _ = (*big.Rat)(&evicted_plan_preparation_time_ms).Float64()
				fields["evicted_plan_size_mb"], _ = (*big.Rat)(&evicted_plan_size_mb).Float64()
				fields["cached_plan_count"] = cached_plan_count
				fields["cached_plan_preparation_time_ms"], _ = (*big.Rat)(&cached_plan_preparation_time_ms).Float64()

				acc.AddFields("hanadb_service_buffer_cache_stats", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

func (m *HanaDB) gatherServiceThreads(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(serviceThreadsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"duration_ms_avg":    0.0,
		"cpu_time_micro_avg": 0.0,
		"count":              0,
	}

	var (
		host                  string
		port                  string
		service_name          string
		workload_class_name   string
		thread_type           string
		thread_method         string
		thread_state          string
		user_name             string
		application_name      string
		application_user_name string
		lock_wait_component   string
		lock_wait_name        string
		is_active             string
		client_ip             string
		cpu_time_micro_avg    driver.Decimal
		duration_ms_avg       driver.Decimal
		count                 int64
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["host"] = hi.host
	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 17 {
			if err := rows.Scan(&host,
				&port,
				&service_name,
				&workload_class_name,
				&thread_type,
				&thread_method,
				&thread_state,
				&user_name,
				&application_name,
				&application_user_name,
				&lock_wait_component,
				&lock_wait_name,
				&is_active,
				&client_ip,
				&duration_ms_avg,
				&cpu_time_micro_avg,
				&count); err == nil {
				tags["host"] = host
				tags["port"] = port
				tags["service_name"] = service_name
				tags["workload_class_name"] = workload_class_name
				tags["thread_type"] = thread_type
				tags["thread_method"] = thread_method
				tags["thread_state"] = thread_state
				tags["user_name"] = user_name
				tags["application_name"] = application_name
				tags["application_user_name"] = application_user_name
				tags["lock_wait_component"] = lock_wait_component
				tags["lock_wait_name"] = thread_method
				tags["is_active"] = is_active
				tags["client_ip"] = thread_method
				fields["duration_ms_avg"], _ = (*big.Rat)(&duration_ms_avg).Float64()
				fields["cpu_time_micro_avg"], _ = (*big.Rat)(&cpu_time_micro_avg).Float64()
				fields["count"] = count

				acc.AddFields("hanadb_service_threads", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

// gatherGatherSDIRemoteSourceStatistics collect SDi remote source stats
func (m *HanaDB) gatherGatherSDIRemoteSourceStatistics(hi *HanaInstance, db *sql.DB, acc telegraf.Accumulator) error {
	// run query
	rows, err := db.Query(sdiRemoteSourceStatisticsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	tags := make(map[string]string)
	fields := map[string]interface{}{
		"statistic_value": 0,
	}

	var (
		remote_source_name string
		service_name       string
		component          string
		sub_component      string
		subscription_name  string
		statistic_number   string
		statistic_name     string
		statistic_value    int64
	)

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	tags["host"] = hi.host
	tags["database_name"] = hi.database_name
	tags["instance_id"] = hi.instance_id
	tags["instance_number"] = hi.instance_number
	// iterate over rows and count the size and count of files
	for rows.Next() {
		if numColumns == 8 {
			if err := rows.Scan(&remote_source_name,
				&service_name,
				&component,
				&sub_component,
				&subscription_name,
				&statistic_number,
				&statistic_name,
				&statistic_value); err == nil {
				tags["remote_source_name"] = remote_source_name
				tags["service_name"] = service_name
				tags["component"] = component
				tags["sub_component"] = sub_component
				tags["subscription_name"] = subscription_name
				tags["statistic_number"] = statistic_number
				tags["statistic_name"] = statistic_name
				fields["statistic_value"] = statistic_value

				//Some stats calculate negative values (values relative to current timetamp, example: last_applied_message_for_subscription_secs)
				if statistic_value < 0 {
					fields["statistic_value"] = 0
				}

				acc.AddFields("hanadb_sdi_remote_source_stats", fields, tags)
			} else {
				return err
			}
		}
	}
	return nil
}

func init() {
	inputs.Add("hanadb", func() telegraf.Input {
		return &HanaDB{
			Server:                                 defaultServer,
			GatherTenanInstances:                   defaultGatherTenanInstances,
			GatherDatabaseDetails:                  defaultGatherDatabaseDetails,
			GatherServiceMemory:                    defaultGatherServiceMemory,
			GatherServicePorts:                     defaultGatherServicePorts,
			GatherConnectionStats:                  defaultGatherConnectionStats,
			GatherSqlServiceStats:                  defaultGatherSqlServiceStats,
			GatherSchemaMemory:                     defaultGatherSchemaMemory,
			CollectInternalSchemaMemory:            defaultCollectInternalSchemaMemory,
			GatherHostResourceUtilization:          defaultGatherHostResourceUtilization,
			GatherHostDiskUsage:                    defaultGatherHostDiskUsage,
			GatherServiceReplication:               defaultGatherServiceReplication,
			GatherCurrentAlerts:                    defaultGatherCurrentAlerts,
			GatherHostAgentCPUMetrics:              defaultGatherHostAgentCPUMetrics,
			GatherHostAgentNetworkMetrics:          defaultGatherHostAgentNetworkMetrics,
			GatherDiskDataFiles:                    defaultGatherDiskDataFiles,
			GatherDiskIO:                           defaultGatherDiskIO,
			GatherInstanceWorkload:                 defaultGatherInstanceWorkload,
			GatherSystemDBDatabases:                defaultGatherSystemDBDatabases,
			GatherTopHeapMemoryCategories:          defaultGatherTopHeapMemoryCategories,
			TopHeapMemoryCategories:                defaultTopHeapMemoryCategories,
			GatherSDIRemoteSubscriptionsStadistics: defaultGatherSDIRemoteSubscriptionsStadistics,
			GatherLicenseUsage:                     defaultGatherLicenseUsage,
			GatherServiceBufferCacheStats:          defaultGatherServiceBufferCacheStats,
			GatherServiceThreads:                   defaultGatherServiceThreads,
			GatherSDIRemoteSourceStatistics:        defaultGatherSDIRemoteSourceStatistics,
		}
	})
}
