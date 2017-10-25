package lookup

import (
	"net/http"
	"github.com/vulcand/oxy/forward"
	log "github.com/sirupsen/logrus"
	"text/template"
	"io"
	"encoding/json"
	"bytes"
)

type Result struct {
	Values map[string]string
}

type Service interface {
	Lookup(req *http.Request) (*Result, error)
}

type Config struct {
	Service Service
	HeaderName string
	SendKeys []string
	HostTemplate *template.Template
	TargetHost string
	TargetScheme string
}

type tpldata struct {
	Request *http.Request
	Lookup map[string]string
}

func NewRequestHandler(config *Config) http.HandlerFunc {

	fwd, err := forward.New(
		forward.PassHostHeader(true),
		forward.Stream(true),
	)
	if err != nil {
		panic(err)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		log.WithFields(log.Fields{
			"host":   req.Host,
			"method": req.Method,
			"uri":    req.RequestURI,
		}).Info("[lookupproxyd] Request")

		result, err := config.Service.Lookup(req)

		if err != nil {
			w.Header().Set("X-Lookup-ErrorType", "lookupFailure")
			w.Header().Set("X-Lookup-Error", err.Error())
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusBadGateway)
			io.WriteString(w, "\n")
			return
		}

		if result == nil {
			w.Header().Set("X-Lookup-ErrorType", "noAccount")
			w.Header().Set("X-Lookup-Error", "Account not found")
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, "\n")
			return
		}

		if len(config.SendKeys) > 0 && config.HeaderName != "" {
			var sendValues map[string]string
			for _, key := range config.SendKeys {
				if val, ok := result.Values[key]; ok {
					sendValues[key] = val
				}
			}

			jsonBytes, err := json.Marshal(sendValues)
			if err != nil {
				w.Header().Set("X-Lookup-ErrorType", "jsonEncodeLookup")
				w.Header().Set("X-Lookup-Error", err.Error())
				w.Header().Set("Content-Type", "text/plain")
				w.WriteHeader(http.StatusBadGateway)
				io.WriteString(w, "\n")
				return
			}

			w.Header().Set(config.HeaderName, string(jsonBytes))
		}

		if config.HostTemplate != nil {
			buf := new(bytes.Buffer)
			config.HostTemplate.Execute(buf, &tpldata{req, result.Values})
			newHost := buf.String()

			req.Header.Set("Host", req.Host)
			req.Host = newHost
		} else {
			req.Header.Set("Host", req.Host)
		}

		newUrl := req.URL
		newUrl.Host = config.TargetHost
		newUrl.Scheme = config.TargetScheme
		req.URL = newUrl

		fwd.ServeHTTP(w, req)
	})
}