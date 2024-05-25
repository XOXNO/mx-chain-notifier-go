package config

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azsecrets"
	"github.com/multiversx/mx-chain-core-go/core"
)


// Configs holds all configs
type Configs struct {
	MainConfig      MainConfig
	ApiRoutesConfig APIRoutesConfig
	Flags           FlagsConfig
}

// MainConfig defines the config setup based on main config file
type MainConfig struct {
	General            GeneralConfig
	WebSocketConnector WebSocketConfig
	ConnectorApi       ConnectorApiConfig
	Redis              RedisConfig
	RabbitMQ           RabbitMQConfig
	AzureServiceBus    AzureServiceBusConfig
	AzureKeyVault      AzureKeyVaultConfig
}

// GeneralConfig maps the general config section
type GeneralConfig struct {
	ExternalMarshaller MarshallerConfig
	AddressConverter   AddressConverterConfig
	CheckDuplicates    bool
}

// MarshallerConfig maps the marshaller configuration
type MarshallerConfig struct {
	Type string
}

// AddressConverterConfig maps the address pubkey converter configuration
type AddressConverterConfig struct {
	Type   string
	Prefix string
	Length int
}

// ConnectorApiConfig maps the connector configuration
type ConnectorApiConfig struct {
	Enabled  bool
	Host     string
	Username string
	Password string
}

// APIRoutesConfig holds the configuration related to Rest API routes
type APIRoutesConfig struct {
	APIPackages map[string]APIPackageConfig
}

// APIPackageConfig holds the configuration for the routes of each package
type APIPackageConfig struct {
	Routes []RouteConfig
}

// RouteConfig holds the configuration for a single route
type RouteConfig struct {
	Name string
	Open bool
	Auth bool
}

// RedisConfig maps the redis configuration
type RedisConfig struct {
	Url            string
	MasterName     string
	SentinelUrl    string
	ConnectionType string
	TTL            uint32
}

// RabbitMQConfig maps the rabbitMQ configuration
type RabbitMQConfig struct {
	Url                     string
	EventsExchange          RabbitMQExchangeConfig
	RevertEventsExchange    RabbitMQExchangeConfig
	FinalizedEventsExchange RabbitMQExchangeConfig
	BlockTxsExchange        RabbitMQExchangeConfig
	BlockScrsExchange       RabbitMQExchangeConfig
	BlockEventsExchange     RabbitMQExchangeConfig
	AlteredAccountsExchange RabbitMQExchangeConfig
}

// RabbitMQExchangeConfig holds the configuration for a rabbitMQ exchange
type RabbitMQExchangeConfig struct {
	Name string
	Type string
}

// WebSocketConfig holds the configuration for websocket observer interaction config
type WebSocketConfig struct {
	Enabled                    bool
	URL                        string
	Mode                       string
	RetryDurationInSec         int
	AcknowledgeTimeoutInSec    int
	WithAcknowledge            bool
	BlockingAckOnError         bool
	DropMessagesIfNoConnection bool

	DataMarshallerType string
}

// FlagsConfig holds the values for CLI flags
type FlagsConfig struct {
	LogLevel          string
	SaveLogFile       bool
	GeneralConfigPath string
	APIConfigPath     string
	WorkingDir        string
	PublisherType     string
	RestApiInterface  string
}

type AzureKeyVaultConfig struct {
	KeyVaultServiceBus  string
	KeyVaultApiUsername string
	KeyVaultApiPassword string
	KeyVaultRabbitMQ    string
	KeyVaultRedisUrl    string
	KeyVaultUrl         string
	Enabled             bool
}

// ServiceBusExchangeConfig holds the configuration for a servicebus exchange
type ServiceBusExchangeConfig struct {
	Topic   string
	Enabled bool
}

type AzureServiceBusConfig struct {
	ServiceBusConnectionString string
	SkipExecutionEventLogs     bool
	ServiceBusTopic            string
	EventsExchange             ServiceBusExchangeConfig
	RevertEventsExchange       ServiceBusExchangeConfig
	FinalizedEventsExchange    ServiceBusExchangeConfig
	BlockTxsExchange           ServiceBusExchangeConfig
	BlockScrsExchange          ServiceBusExchangeConfig
	BlockEventsExchange        ServiceBusExchangeConfig
	AlteredAccountsExchange    ServiceBusExchangeConfig
}

// LoadMainConfig returns a MainConfig instance by reading the provided toml file
func LoadMainConfig(filePath string) (*MainConfig, error) {
	cfg := &MainConfig{}
	err := core.LoadTomlFile(cfg, filePath)
	if err != nil {
		return nil, err
	}

	if cfg.AzureKeyVault.Enabled {
		credentials, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}

		client, err := azsecrets.NewClient(cfg.AzureKeyVault.KeyVaultUrl, credentials, nil)
		if err != nil {
			return nil, err
		}

		if cfg.AzureKeyVault.KeyVaultRabbitMQ != "" {
			rabbitURL, err := client.GetSecret(context.Background(), cfg.AzureKeyVault.KeyVaultRabbitMQ, "", nil)
			if err != nil {
				return nil, err
			}
			cfg.RabbitMQ.Url = *rabbitURL.Value
		}

		if cfg.AzureKeyVault.KeyVaultRedisUrl != "" {
			redisURL, err := client.GetSecret(context.Background(), cfg.AzureKeyVault.KeyVaultRedisUrl, "", nil)
			if err != nil {
				return nil, err
			}
			cfg.Redis.Url = *redisURL.Value
		}

		if cfg.AzureKeyVault.KeyVaultApiUsername != "" {
			apiUsername, err := client.GetSecret(context.Background(), cfg.AzureKeyVault.KeyVaultApiUsername, "", nil)
			if err != nil {
				return nil, err
			}
			cfg.ConnectorApi.Username = *apiUsername.Value
		}

		if cfg.AzureKeyVault.KeyVaultApiPassword != "" {
			apiPassword, err := client.GetSecret(context.Background(), cfg.AzureKeyVault.KeyVaultApiPassword, "", nil)
			if err != nil {
				return nil, err
			}
			cfg.ConnectorApi.Password = *apiPassword.Value
		}

		if cfg.AzureKeyVault.KeyVaultServiceBus != "" {
			serviceBusUrl, err := client.GetSecret(context.Background(), cfg.AzureKeyVault.KeyVaultServiceBus, "", nil)
			if err != nil {
				return nil, err
			}
			cfg.AzureServiceBus.ServiceBusConnectionString = *serviceBusUrl.Value
		}
	}

	return cfg, err
}

// LoadAPIConfig returns a APIRoutesConfig instance by reading the provided toml file
func LoadAPIConfig(filePath string) (*APIRoutesConfig, error) {
	cfg := &APIRoutesConfig{}
	err := core.LoadTomlFile(cfg, filePath)
	if err != nil {
		return nil, err
	}
	return cfg, err
}
