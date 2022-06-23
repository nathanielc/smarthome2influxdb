package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nathanielc/smarthome"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var runFlags = struct {
	mqttURL     string
	influxDBURL string
	bucket      string
	org         string
	token       string
}{}

func main() {

	var cmdRun = &cobra.Command{
		Use:   "run",
		Short: "Listen to smarthome MQTT messages and record in InfluxDB",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			v := viper.New()
			v.SetEnvPrefix("SH2INFLUX")
			v.AutomaticEnv()

			// Bind each cobra flag to its associated viper configuration (config file and environment variable)
			cmd.Flags().VisitAll(func(f *pflag.Flag) {
				// Apply the viper config value to the flag when the flag is not set and viper has a value
				if !f.Changed && v.IsSet(f.Name) {
					val := v.Get(f.Name)
					cmd.Flags().Set(f.Name, fmt.Sprintf("%v", val))
				}
			})
			return nil
		},
		RunE: run,
	}
	cmdRun.Flags().StringVarP(&runFlags.mqttURL, "mqtt", "m", "tcp://localhost:1883", "URL for connecting to MQTT broker, can be set with MQTT_URL")
	cmdRun.Flags().StringVarP(&runFlags.influxDBURL, "influxdb", "i", "http://localhost:8086", "URL for connecting to InfluxDB database, can be set with env INFLUXDB_URL")
	cmdRun.Flags().StringVarP(&runFlags.bucket, "bucket", "b", "smarthome", "Name of InfluxDB bucket, can be set with env INFLUXDB_BUCKET")
	cmdRun.Flags().StringVarP(&runFlags.org, "org", "o", "", "Name of InfluxDB organization, can be set with env INFLUXDB_ORG")
	cmdRun.Flags().StringVarP(&runFlags.token, "token", "t", "", "Name of InfluxDB organization, can be set with env INFLUXDB_TOKEN")

	var rootCmd = &cobra.Command{
		Use: "smarthome2influxdb",
	}
	rootCmd.AddCommand(cmdRun)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var bp = newBufferPool()
var writeURL string

func run(cmd *cobra.Command, args []string) error {
	q := make(url.Values)
	q.Add("bucket", runFlags.bucket)
	q.Add("org", runFlags.org)
	writeURL = runFlags.influxDBURL + "/api/v2/write?" + q.Encode()
	log.Printf("writeURL: %s", writeURL)
	c, err := connect(runFlags.mqttURL)
	if err != nil {
		return err
	}
	defer c.Disconnect(150)
	err = subscribe(c)
	if err != nil {
		return err
	}

	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, syscall.SIGKILL)
	<-signals
	return nil
}

func connect(addr string) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		SetKeepAlive(5 * time.Second).
		SetAutoReconnect(true).
		AddBroker(addr).
		SetClientID("mqtt2influxdb")

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	return c, nil
}

func subscribe(c mqtt.Client) error {
	if token := c.Subscribe("+/status/#", 0, doStatus); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	if token := c.Subscribe("+/connected", 0, doConnected); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

const (
	statusPath    = "/status/"
	connectedPath = "/connected"
)

func doStatus(c mqtt.Client, m mqtt.Message) {
	buf := bp.Get()
	defer bp.Put(buf)
	v := smarthome.PayloadToValue(m.Payload())
	if v.Time.IsZero() {
		v.Time = time.Now()
	}
	topic := m.Topic()
	i := strings.Index(topic, statusPath)
	toplevel := topic[0:i]
	path := topic[i+len(statusPath):]
	buf.WriteString("status,toplevel=")
	buf.WriteString(toplevel)
	buf.WriteString(",path=")
	buf.WriteString(path)
	buf.WriteString(" value=")
	switch val := v.Value.(type) {
	case string:
		buf.WriteRune('"')
		buf.WriteString(strings.Replace(val, `"`, `\"`, -1))
		buf.WriteRune('"')
	case float64:
		buf.WriteRune('"')
		buf.WriteString(strconv.FormatFloat(val, 'f', -1, 64))
		buf.WriteRune('"')
	default:
		buf.WriteRune('"')
		buf.WriteString(strings.Replace(fmt.Sprintf("%v", val), `"`, `\"`, -1))
		buf.WriteRune('"')
	}
	buf.WriteRune(' ')
	buf.WriteString(strconv.FormatInt(v.Time.UnixNano(), 10))
	buf.WriteRune('\n')
	log.Println("writing status event:", buf.String())
	err := write(buf)
	if err != nil {
		log.Printf("error writing status event: %s", err)
	}
}

func doConnected(c mqtt.Client, m mqtt.Message) {
	buf := bp.Get()
	defer bp.Put(buf)
	v := smarthome.PayloadToValue(m.Payload())
	if v.Time.IsZero() {
		v.Time = time.Now()
	}
	topic := m.Topic()
	i := strings.Index(topic, connectedPath)
	toplevel := topic[0:i]
	buf.WriteString("connected,toplevel=")
	buf.WriteString(toplevel)
	buf.WriteString(" value=")
	switch val := v.Value.(type) {
	case string:
		buf.WriteString(val)
	case float64:
		buf.WriteString(strconv.Itoa(int(val)))
	}
	buf.WriteRune('i')
	buf.WriteRune(' ')
	buf.WriteString(strconv.FormatInt(v.Time.UnixNano(), 10))
	buf.WriteRune('\n')
	log.Println("writing connected event: ", buf.String())
	err := write(buf)
	if err != nil {
		log.Printf("error writing connected event: %s", err)
	}
}

func write(data io.Reader) error {
	req, err := http.NewRequest("POST", writeURL, data)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+runFlags.token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected HTTP status code: %d", res.StatusCode)
	}

	return res.Body.Close()
}

type bufferPool sync.Pool

func newBufferPool() *bufferPool {
	return &bufferPool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}
}

func (b *bufferPool) Get() *bytes.Buffer {
	buf := (*sync.Pool)(b).Get()
	return buf.(*bytes.Buffer)
}
func (b *bufferPool) Put(buf *bytes.Buffer) {
	buf.Reset()
	(*sync.Pool)(b).Put(buf)
}
