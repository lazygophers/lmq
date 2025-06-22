package state

import (
	"github.com/lazygophers/log"
	"github.com/lazygophers/utils/app"
	"github.com/lazygophers/utils/config"
	"github.com/lazygophers/utils/runtime"
	"path/filepath"
)

type Config struct {
	Name string `json:"name,omitempty" yaml:"name,omitempty" toml:"name,omitempty"`

	Port int    `json:"port,omitempty" yaml:"port,omitempty" toml:"port,omitempty"`
	Host string `json:"host,omitempty" yaml:"host,omitempty" toml:"host,omitempty"`
	// NOTE: Please fill in the configuration below

	DataPath string `json:"data_path,omitempty" yaml:"data_path,omitempty" toml:"data_path,omitempty"`
}

func (p *Config) apply() {
	if p.DataPath == "" {
		p.DataPath = filepath.Join(runtime.LazyCacheDir(), "lmq")
	}
}

func LoadConfig() (err error) {
	State.Config = new(Config)
	err = config.LoadConfig(State.Config)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if app.Name == "" {
		app.Name = State.Config.Name
	}

	State.Config.apply()

	return nil
}
