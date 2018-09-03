package conf

type Configuration struct {
	Tunnel        string `config:"tunnel"`
	TunnelAddress string `config:"tunnel.address"`
	SystemProfile int    `config:"system_profile"`
	LogLevel      string `config:"log_level"`
	LogFileName   string `config:"log_file"`
	LogBuffer     bool   `config:"log_buffer"`
	ReplayerNum   int    `config:"replayer"`
}

var Options Configuration
