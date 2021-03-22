package conf

type Configuration struct {
	Tunnel            string `config:"tunnel"`
	TunnelAddress     string `config:"tunnel.address"`
	SystemProfilePort int    `config:"system_profile_port"`
	LogDirectory      string `config:"log.dir"`
	LogLevel          string `config:"log.level"`
	LogFileName       string `config:"log.file"`
	LogFlush          bool   `config:"log.flush"`
	ReplayerNum       int    `config:"replayer"`
}

var Options Configuration
