package importer

import (
	"flag"
	"fmt"

	"github.com/BurntSushi/toml"
)

func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("importer", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.configFile, "config", "", "Config file")
	fs.StringVar(&cfg.tableName, "table", "", "Table name")
	fs.StringVar(&cfg.dataFile, "data", "", "Data file")

	fs.IntVar(&cfg.WorkerCount, "c", 2, "parallel worker count")

	fs.StringVar(&cfg.DBCfg.Host, "h", "127.0.0.1", "set the database host ip")
	fs.StringVar(&cfg.DBCfg.User, "u", "root", "set the database user")
	fs.StringVar(&cfg.DBCfg.Password, "p", "", "set the database password")
	fs.StringVar(&cfg.DBCfg.Name, "D", "test", "set the database name")
	fs.IntVar(&cfg.DBCfg.Port, "P", 3306, "set the database host port")

	return cfg
}

type DBConfig struct {
	Host     string `toml:"host" json:"host"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"password"`
	Name     string `toml:"name" json:"name"`
	Port     int    `toml:"port" json:"port"`
}

func (c *DBConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DBConfig(%+v)", *c)
}

type Config struct {
	*flag.FlagSet `json:"-"`

	DBCfg       DBConfig `toml:"db" json:"db"`
	WorkerCount int      `toml:"worker-count" json:"worker-count"`

	configFile string

	tableName string
	dataFile  string
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return err
	}

	// Load config file if specified.
	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return err
	}

	if len(c.FlagSet.Args()) != 0 {
		return fmt.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}

func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return err
}
