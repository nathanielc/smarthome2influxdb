package main

import (
	"bytes"
	"flag"
	"fmt"
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
)

var mqttURL = flag.String("mqtt", "tcp://localhost:1883", "URL for connecting to MQTT broker")
var influxDBURL = flag.String("influxdb", "http://localhost:8086", "URL for connecting to InfluxDB database")
var database = flag.String("database", "mqtt-smarthome", "Name of InfluxDB database")

var bp = newBufferPool()

var writeURL string

func main() {
	flag.Parse()

	q := url.Values{}
	q.Set("db", *database)
	writeURL = *influxDBURL + "/write?" + q.Encode()
	c, err := connect(*mqttURL)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Disconnect(150)
	err = subscribe(c)
	if err != nil {
		log.Fatal(err)
	}

	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, syscall.SIGKILL)
	<-signals
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
	fmt.Println(buf.String())
	req, err := http.Post(writeURL, "application/octet-stream", buf)
	if err != nil {
		log.Println(err)
		return
	}
	req.Body.Close()
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
	fmt.Println(m.Topic(), string(m.Payload()))
	fmt.Println(v, buf.String())
	req, err := http.Post(writeURL, "application/octet-stream", buf)
	if err != nil {
		log.Println(err)
		return
	}
	req.Body.Close()
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
