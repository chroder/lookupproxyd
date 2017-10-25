package redislookup

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"net/http"
	"encoding/json"
	"sync"
	"github.com/chroder/lookupproxyd/lookup"
	log "github.com/sirupsen/logrus"
)

type Service struct {
	pool *redis.Pool
	errTimer *errTimer
	retries int
}

type errTimer struct {
	lastErrorTime time.Time
	mu sync.Mutex
}

type Pool redis.Pool

func New(pool *Pool, retries int) (*Service, error) {
	et := &errTimer{
		lastErrorTime: time.Now().Add(- time.Second * 60),
	}

	pool.buildBorrowTest(et)

	// Convert our pool with custom borrow check
	// to the normal redislookup pool
	rPool := (*redis.Pool)(pool)

	testConn := rPool.Get()
	defer testConn.Close()

	_, err := testConn.Do("PING")
	if err != nil {
		return nil, err
	}

	return &Service{
		pool: rPool,
		errTimer: et,
		retries: retries,
	}, nil
}


func (s *Service) Lookup(req *http.Request) (*lookup.Result, error) {
	var res *lookup.Result
	var err error

	for i := 0; i <= s.retries; i++ {
		res, err = s.doLookup(req)

		if err == nil {
			return res, nil
		}
	}

	if err == nil {
		return res, nil
	}

	return res, nil
}

func (s *Service) doLookup(req *http.Request) (*lookup.Result, error) {
	conn := s.pool.Get()
	defer conn.Close()

	key := "domain:" + req.Host
	v, err := conn.Do("GET", key)

	if err != nil {
		// lookup error
		log.WithFields(log.Fields{"error": err, "key": key}).Warn("Lookup error")
		s.errTimer.markError()
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	bytes, err := redis.Bytes(v, nil)

	if err != nil {
		return nil, err
	}

	values := make(map[string]string)
	err = json.Unmarshal(bytes, &values)

	if err != nil {
		log.WithFields(log.Fields{"error": err, "key": key}).Warn("JSON decode error")
		return nil, err
	}

	return &lookup.Result{Values: values}, nil
}

func (p *Pool) buildBorrowTest(et *errTimer) {
	existBorrowTest := p.TestOnBorrow
	p.TestOnBorrow = func(c redis.Conn, t time.Time) error {
		var err error

		// Check user-supplied borrow test first
		if existBorrowTest != nil {
			err = existBorrowTest(c, t)
			if err != nil {
				return err
			}
		}

		// if there's been an error since the last time
		// this connection was borrowed, we'll
		// check it with a ping.
		//
		// This catches a possibility where all redislookup
		// connections died simultaneously, we'll want
		// to make sure a connection is alive before we
		// try to use it.
		//
		// We only do this check after an error though,
		// so we are optimistic until an error, then we check

		et.mu.Lock()
		needsPingCheck := t.Before(et.lastErrorTime)
		et.mu.Unlock()

		if needsPingCheck {
			_, err := c.Do("PING")
			return err
		}

		return nil
	}
}

func (et *errTimer) markError() {
	et.mu.Lock()
	et.lastErrorTime = time.Now()
	et.mu.Unlock()
}