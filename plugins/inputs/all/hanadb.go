//go:build !custom || inputs || inputs.hanadb

package all

import _ "github.com/influxdata/telegraf/plugins/inputs/hanadb" // register plugin
