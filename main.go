package main

import (
	"errors"
	"fmt"
	"github.com/chroder/lookupproxyd/lookup"
	"github.com/chroder/lookupproxyd/lookup/redislookup"
	"github.com/garyburd/redigo/redis"
	log "github.com/sirupsen/logrus"
	"gopkg.in/urfave/cli.v2"
	"net/http"
	"os"
	"strings"
	"text/template"
	"time"
)

func run(c *cli.Context) error {
	err := validateCliOptions(c)
	if err != nil {
		println(err.Error())
		return cli.Exit("Check your comand-line options or use --help.", 1)
	}

	logLvl, _ := log.ParseLevel(c.String("log-level"))
	log.SetLevel(logLvl)

	fmt.Println("[lookupproxyd] Starting...")
	fmt.Printf("[lookupproxyd] Listening on \t%s\n", c.String("listen"))
	fmt.Printf("[lookupproxyd] Redis host   \t%s\n", c.String("redis-host"))

	fmt.Printf("[lookupproxyd] Proxying to  \t%s (%s)\n", c.String("target-host"), c.String("target-scheme"))

	var targetHostTpl *template.Template
	fmt.Printf("[lookupproxyd] Target pattern  \t%s\n", c.String("target-host"))
	targetHostTpl, err = template.New("target-host").Parse(c.String("target-host"))
	if err != nil {
		println(err.Error())
		return cli.Exit("The target host template you entered is invalid", 21)
	}

	var hostTpl *template.Template
	if c.String("rewrite-host") != "" {
		fmt.Printf("[lookupproxyd] Host pattern  \t%s\n", c.String("rewrite-host"))
		hostTpl, err = template.New("rewrite-host").Parse(c.String("rewrite-host"))
		if err != nil {
			println(err.Error())
			return cli.Exit("The host template you entered is invalid", 21)
		}
	}

	var pathTpl *template.Template
	if c.String("rewrite-path") != "" {
		fmt.Printf("[lookupproxyd] Path pattern  \t%s\n", c.String("rewrite-path"))
		pathTpl, err = template.New("rewrite-host").Parse(c.String("rewrite-path"))
		if err != nil {
			println(err.Error())
			return cli.Exit("The path template you entered is invalid", 21)
		}
	}

	service, err := createRedisLookup(c)
	if err != nil {
		println(err.Error())
		return cli.Exit("Failed to create lookup service.", 20)
	}

	handler := lookup.NewRequestHandler(&lookup.Config{
		Service:            service,
		HeaderName:         c.String("send-header"),
		SendKeys:           c.StringSlice("send-keys"),
		HostTemplate:       hostTpl,
		PathTemplate:       pathTpl,
		TargetHostTemplate: targetHostTpl,
		TargetScheme:       c.String("target-scheme"),
	})

	s := &http.Server{
		Addr:    c.String("listen"),
		Handler: handler,
	}
	err = s.ListenAndServe()

	if err != nil {
		log.WithFields(log.Fields{"error": err}).Fatal("[lookupproxyd] Failed to start")
		os.Exit(50)
	}

	return nil
}

func createRedisLookup(c *cli.Context) (*redislookup.Service, error) {
	host := c.String("redis-host")
	auth := c.String("redis-auth")

	pool := &redislookup.Pool{
		MaxIdle:     20,
		MaxActive:   5000,
		IdleTimeout: time.Duration(1) * time.Minute,
		Wait:        false,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial(
				"tcp",
				host,
				redis.DialConnectTimeout(time.Duration(250)*time.Millisecond),
				redis.DialReadTimeout(time.Duration(500)*time.Millisecond),
				redis.DialWriteTimeout(time.Duration(500)*time.Millisecond),
			)
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Warn("[lookupproxyd] Redis: Dial failure")
				return nil, err
			}
			if auth != "" {
				if _, err := c.Do("AUTH", auth); err != nil {
					c.Close()
					log.WithFields(log.Fields{"error": err}).Warn("[lookupproxyd] Redis: Auth failure")
					return nil, err
				}
			}
			log.Debug("[lookupproxyd] New redis connection")
			return c, nil
		},
	}

	return redislookup.New(pool, 4)
}

func validateCliOptions(c *cli.Context) error {
	var errStrings []string

	if c.String("redis-host") == "" {
		errStrings = append(errStrings, "--redis-host is required")
	}
	if c.String("target-host") == "" {
		errStrings = append(errStrings, "--target-host is required")
	}
	if c.String("target-scheme") != "http" && c.String("target-scheme") != "https" {
		errStrings = append(errStrings, "--target-scheme must be http or https")
	}

	_, err := log.ParseLevel(c.String("log-level"))
	if err != nil {
		errStrings = append(errStrings, "--log-level must be one of: debug, info, warning, error, fatal, panic")
	}

	if len(errStrings) > 0 {
		return errors.New("\t" + strings.Join(errStrings, "\n\t") + "\n")
	}

	return nil
}

func main() {
	app := &cli.App{
		Name:  "lookupproxyd",
		Usage: "Listens for HTTP requests performs a lookup in a Redis database, modifies Host and adds an X-header with optional values, then forwards it to a backend.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "listen",
				Value:   ":8100",
				Aliases: []string{"i"},
				Usage:   "The address to listen on. E.g. :8100 or 1.2.3.4:8100 for a particular interface",
			},
			&cli.StringFlag{
				Name:    "redis-host",
				Aliases: []string{"r"},
				Usage:   "The Redis host. E.g. 1.2.3.4:6379",
			},
			&cli.StringFlag{
				Name:    "redis-auth",
				Aliases: []string{"a"},
				Usage:   "If the Redis server requires auth",
			},
			&cli.StringFlag{
				Name:    "target-host",
				Aliases: []string{"t"},
				Usage:   "The target host to forward to. E.g. 1.2.3.4:80. This is a template can variables can be used. E.g. 1.2.3.4:{{.Lookup.XXX}}. Note: Not a URL; don't enter protocol.",
			},
			&cli.StringFlag{
				Name:    "target-scheme",
				Value:   "http",
				Aliases: []string{"m"},
				Usage:   "The target scheme to use (http or https).",
			},
			&cli.StringFlag{
				Name:    "log-level",
				Value:   "warning",
				Aliases: []string{"l"},
				Usage:   "Specify: debug, info, warning, error, fatal, panic",
			},
			&cli.StringFlag{
				Name:    "rewrite-host",
				Value:   "",
				Aliases: []string{"w"},
				Usage:   "Specify a template to rewrite the Host as. The provided host will be in {{.Request.Host}} and data from the lookup in {{.Lookup.XXX}}. If you do not specify a value, no host rewrite will happen.",
			},
			&cli.StringFlag{
				Name:    "rewrite-path",
				Value:   "",
				Aliases: []string{"p"},
				Usage:   "Specify a template to rewrite the Path as. The provided path will be in {{.Request.URL.Path}} and data from the lookup in {{.Lookup.XXX}}. If you do not specify a value, no path rewrite will happen.",
			},
			&cli.StringSliceFlag{
				Name:    "send-keys",
				Value:   nil,
				Aliases: []string{"s"},
				Usage:   "Specify a list of keys to send in the X-header (JSON encoded). If none provided, or the keys don't exist in the lookup, no X-header will be sent.",
			},
			&cli.StringFlag{
				Name:    "send-header",
				Value:   "X-Lookup",
				Aliases: []string{"x"},
				Usage:   "Specify the name of the X-header to send values in.",
			},
			&cli.BoolFlag{
				Name:    "trust-upstream",
				Value:   false,
				Aliases: []string{"u"},
				Usage:   "Set this to trust the upstream host (trusts X-Forwarded headers and passes them on).",
			},
		},
		Action: run,
	}

	app.Run(os.Args)
}
