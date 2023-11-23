//go:build linux
// +build linux

package net

import (
	"fmt"
	"hash/crc32"
	"strconv"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/system"
	"github.com/shirou/gopsutil/process"
	sockstats "github.com/superguillen/socket-collector/net"
)

type NetStatsConnections struct {
	ps                             system.PS
	Log                            telegraf.Logger
	ListenProcessDetail            bool     `toml:"listen_process_detail"`
	RemoteConnections              bool     `toml:"remote_connections"`
	RemoteConnectionsProcessDetail bool     `toml:"remote_connections_process_detail"`
	SocketsStatsEnabled            bool     `toml:"sockets_stats_enabled"`
	SocketsStatsGroupMetrics       string   `toml:"sockets_stats_group_metrics"`
	SocketsStatsOperations         string   `toml:"sockets_stats_operations"`
	SocketsStatsCustomMetrics      []string `toml:"sockets_stats_custom_metrics"`
}

type PortData struct {
	Pid                     int32
	ProcessName             string
	Username                string
	ProcessStatus           string
	ExecutablePath          string
	CurrentWorkingDirectory string
	CommandLineArguments    string
	Local_addr              string
	Local_port              uint32
	Remote_addr             string
	Remote_port             uint32
	Status                  string
	Established             int
	SynSent                 int
	SynRecv                 int
	FinWait1                int
	FinWait2                int
	TimeWait                int
	Close                   int
	CloseWait               int
	LastAck                 int
	Listen                  int
	Closing                 int
	None                    int
	UDP                     int
	Count                   int
}

func (_ *NetStatsConnections) Description() string {
	return "Read TCP metrics such as established, time wait and sockets counts."
}

var tcpstatProcessSampleConfig = `
  ## By default, telegraf don't collect process detail of listen connections
  ## to enable collect this metric to telegraf agent will be enable following OS capabilitiess: CAP_SYS_PTRACE, CAP_DAC_READ_SEARCH
  ## example: setcap cap_sys_ptrace,cap_dac_read_search+ep /usr/bin/telegraf
  ##
  # listen_process_detail = true
  ##
  ## By default, telegraf don't collect detail of outgoing connections
  ##
  # remote_connections = true
  ##
  ## To enable collect this metric to telegraf agent will be enable following OS capabilitiess: CAP_SYS_PTRACE, CAP_DAC_READ_SEARCH
  ##
  # remote_connections_process_detail = true
  ##
  ## By default, telegraf don't collect Socket stats (high cardinality)
  ##
  # sockets_stats_enabled = true
  ### Collect BASIC group of metrics (use "FULL" to collect all avaliable TCPINFO kernel stats)
  # sockets_stats_group_metrics = "BASIC"
  ### If need Collect CUSTOM group of metrics (for example Notsent_bytes), use "CUSTOM" with sockets_stats_custom_metrics parameter.
  # sockets_stats_group_metrics = "CUSTOM"
  # sockets_stats_custom_metrics = ["Segs_out","Notsent_bytes","Rtt"]
  ### Collect BASIC group of stadistics operations ["mean", "p99"] (use "FULL" to collect max,mix,mean,p75,p95,p99)
  # sockets_stats_operations = "BASIC"
  ##
`

func (_ *NetStatsConnections) SampleConfig() string {
	return tcpstatProcessSampleConfig
}

func (s *NetStatsConnections) Gather(acc telegraf.Accumulator) error {
	var globalConnStadistics sockstats.GlobalConnStatistics
	netconns, err := s.ps.NetConnections()

	if err != nil {
		return fmt.Errorf("error getting net connections info: %s", err)
	}

	//Only collect sockstats if is enabled
	if s.SocketsStatsEnabled {
		metrics := sockstats.BASIC_METRICS
		stats := sockstats.BASIC_STATS
		group_stats := sockstats.BasicStatsMetrics

		s.Log.Info("Socket Stadistics collection enabled")

		switch s.SocketsStatsOperations {
		case "FULL":
			stats = sockstats.FULL_STATS
			group_stats = sockstats.FullStatsMetrics
			s.Log.Infof("Socket Stadistics switched to FULL_METRICS stats collection.")
		default:
			s.Log.Infof("Socket Stadistics switched to BASIC stats collection.")
		}

		switch s.SocketsStatsGroupMetrics {
		case "FULL":
			metrics = sockstats.FULL_METRICS
			s.Log.Infof("Socket Stadistics switched to FULL_METRICS metric collection.")
		case "CUSTOM":
			metrics = s.SocketsStatsCustomMetrics
			s.Log.Infof("Socket Stadistics switched to CUSTOM metric collection. (please validate metric names)")
		default:
			s.Log.Infof("Socket Stadistics switched to BASIC metric collection.")
		}

		s.Log.Infof("Socket Stadistics to collect:")

		metric_count := 0
		for _, metric := range metrics {
			if stat, ok := group_stats[metric]; ok {
				metric_count = 1
				s.Log.Infof("metric [%v] stats: %+v", metric, stat)
			} else {
				s.Log.Warnf("Metric [%v] is not valid, please review documentation.", metric)
			}
		}

		if metric_count == 0 {
			s.Log.Warnf("Socket Stadistics has no valid metric names, swiched to BASIC metric collection.")
			metrics = sockstats.BASIC_METRICS
		}

		globalConnStadistics, err = sockstats.GetConnStatistics(metrics, stats)

		if err != nil {
			return fmt.Errorf("error getting socket stats: %s", err)
		}
	}

	listen_ports := make(map[string]*PortData)
	connected_ports := make(map[string]*PortData)

	for _, netcon := range netconns {
		var port = ""
		//var proc *process.Process
		switch netcon.Status {
		case "LISTEN":
			port = strconv.Itoa(int(netcon.Laddr.Port))
			c, ok := listen_ports[port]
			if !ok {
				var process_name string
				var username string
				var process_status string
				var executable_path string
				var current_working_directory string
				var command_line_args string

				if s.ListenProcessDetail {
					proc, err := process.NewProcess(netcon.Pid)
					if err == nil {
						process_name, _ = proc.Name()
						username, _ = proc.Username()
						process_status, _ = proc.Status()
						executable_path, _ = proc.Exe()
						current_working_directory, _ = proc.Cwd()
						command_line_args, _ = proc.Cmdline()
					}
				}

				c = &PortData{
					Pid:                     netcon.Pid,
					ProcessName:             process_name,
					Username:                username,
					ProcessStatus:           process_status,
					ExecutablePath:          executable_path,
					CurrentWorkingDirectory: current_working_directory,
					CommandLineArguments:    command_line_args,
					Local_addr:              netcon.Laddr.IP,
					Local_port:              netcon.Laddr.Port,
					Remote_addr:             netcon.Raddr.IP,
					Remote_port:             netcon.Raddr.Port,
					Status:                  netcon.Status,
					Listen:                  1,
				}
				listen_ports[port] = c
			} else {
				c.Listen += 1
			}
		}
	}
	for _, netcon := range netconns {
		var port = ""
		if netcon.Status != "LISTEN" {
			//Add count status by listen port
			port = strconv.Itoa(int(netcon.Laddr.Port))
			c, ok := listen_ports[port]
			if ok {
				switch netcon.Status {
				case "ESTABLISHED":
					c.Established += 1
				case "SYN_SENT":
					c.SynSent += 1
				case "SYN_RECV":
					c.SynRecv += 1
				case "FIN_WAIT1":
					c.FinWait1 += 1
				case "FIN_WAIT2":
					c.FinWait2 += 1
				case "TIME_WAIT":
					c.TimeWait += 1
				case "CLOSE":
					c.Close += 1
				case "CLOSE_WAIT":
					c.CloseWait += 1
				case "LAST_ACK":
					c.LastAck += 1
				case "CLOSING":
					c.Closing += 1
				case "NONE":
					c.None += 1
				}
			} else if s.RemoteConnections && netcon.Raddr.Port > 0 { //Only generate remote connectios by parameter
				var process_name string
				var username string
				var process_status string
				var executable_path string
				var current_working_directory string
				var command_line_args string
				var command_line_args_checksum string

				if s.RemoteConnectionsProcessDetail {
					proc, err := process.NewProcess(netcon.Pid)
					if err == nil {
						process_name, _ = proc.Name()
						username, _ = proc.Username()
						process_status, _ = proc.Status()
						executable_path, _ = proc.Exe()
						current_working_directory, _ = proc.Cwd()
						command_line_args, _ = proc.Cmdline()
						command_line_args_checksum = strconv.FormatUint(uint64(crc32.ChecksumIEEE([]byte(command_line_args))), 10)
					}
				}

				port = netcon.Raddr.IP + "_" + username + "_" + process_name + "_" + command_line_args_checksum + "_" + strconv.Itoa(int(netcon.Raddr.Port))
				c, ok := connected_ports[port]
				if !ok {
					c = &PortData{
						Pid:                     netcon.Pid,
						ProcessName:             process_name,
						Username:                username,
						ProcessStatus:           process_status,
						ExecutablePath:          executable_path,
						CurrentWorkingDirectory: current_working_directory,
						CommandLineArguments:    command_line_args,
						Local_addr:              netcon.Laddr.IP,
						Local_port:              netcon.Laddr.Port,
						Remote_addr:             netcon.Raddr.IP,
						Remote_port:             netcon.Raddr.Port,
						Status:                  netcon.Status,
					}
					connected_ports[port] = c
				}
				switch netcon.Status {
				case "ESTABLISHED":
					c.Established += 1
				case "SYN_SENT":
					c.SynSent += 1
				case "SYN_RECV":
					c.SynRecv += 1
				case "FIN_WAIT1":
					c.FinWait1 += 1
				case "FIN_WAIT2":
					c.FinWait2 += 1
				case "TIME_WAIT":
					c.TimeWait += 1
				case "CLOSE":
					c.Close += 1
				case "CLOSE_WAIT":
					c.CloseWait += 1
				case "LAST_ACK":
					c.LastAck += 1
				case "CLOSING":
					c.Closing += 1
				case "NONE":
					c.None += 1
				}
			}
		}
	}

	for _, value := range listen_ports {
		acc.AddFields("netstat_incoming",
			map[string]interface{}{
				"tcp_established": value.Established,
				"tcp_syn_send":    value.SynSent,
				"tcp_syn_recv":    value.SynRecv,
				"tcp_fin_wait1":   value.FinWait1,
				"tcp_fin_wait2":   value.FinWait2,
				"tcp_time_wait":   value.TimeWait,
				"tcp_close":       value.Close,
				"tcp_close_wait":  value.CloseWait,
				"tcp_last_ack":    value.LastAck,
				"tcp_listen":      value.Listen,
				"tcp_closing":     value.Closing,
				"tcp_none":        value.None,
			},
			map[string]string{
				//"pid": strconv.Itoa(int(value.Pid)),
				"local_addr":                value.Local_addr,
				"port":                      strconv.Itoa(int(value.Local_port)),
				"process_name":              value.ProcessName,
				"username":                  value.Username,
				"current_working_directory": value.CurrentWorkingDirectory,
				"command_line_arguments":    value.CommandLineArguments,
				//"remote_addr": value.Remote_addr,
				//"remote_port": strconv.Itoa(int(value.Remote_port)),
				//"status": value.Status,
			})
	}
	for _, value := range connected_ports {
		acc.AddFields("netstat_outgoing",
			map[string]interface{}{
				"tcp_established": value.Established,
				"tcp_syn_send":    value.SynSent,
				"tcp_syn_recv":    value.SynRecv,
				"tcp_fin_wait1":   value.FinWait1,
				"tcp_fin_wait2":   value.FinWait2,
				"tcp_time_wait":   value.TimeWait,
				"tcp_close":       value.Close,
				"tcp_close_wait":  value.CloseWait,
				"tcp_last_ack":    value.LastAck,
				"tcp_listen":      value.Listen,
				"tcp_closing":     value.Closing,
				"tcp_none":        value.None,
			},
			map[string]string{
				//"pid": strconv.Itoa(int(value.Pid)),
				//"local_addr": value.Local_addr,
				//"local_port": strconv.Itoa(int(value.Local_port)),
				"addr":                      value.Remote_addr,
				"port":                      strconv.Itoa(int(value.Remote_port)),
				"process_name":              value.ProcessName,
				"username":                  value.Username,
				"current_working_directory": value.CurrentWorkingDirectory,
				"command_line_arguments":    value.CommandLineArguments,
				//"status": value.Status,
			})
	}

	if s.SocketsStatsEnabled {
		//Incoming conections
		for port, value := range globalConnStadistics.IncomingConns {
			acc.AddFields("sockstats_incoming",
				value.TCPInfoStats,
				map[string]string{
					"local_addr": port.Addr,
					"port":       strconv.FormatUint(uint64(port.PortNum), 10),
				})
		}
		if s.RemoteConnections {
			//OutGoing conections
			for port, value := range globalConnStadistics.OutgoingConns {
				acc.AddFields("sockstats_outgoing",
					value.TCPInfoStats,
					map[string]string{
						"addr": port.Addr,
						"port": strconv.FormatUint(uint64(port.PortNum), 10),
					})
			}
		}
	}

	return nil
}

func init() {
	inputs.Add("netstat_connections", func() telegraf.Input {
		return &NetStatsConnections{ps: system.NewSystemPS(),
			RemoteConnections:         false,
			SocketsStatsEnabled:       false,
			SocketsStatsGroupMetrics:  "BASIC",
			SocketsStatsOperations:    "BASIC",
			SocketsStatsCustomMetrics: []string{}}
	})
}

