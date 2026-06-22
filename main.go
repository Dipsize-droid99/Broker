package main

import (
	"flag"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type waiter struct {
	ch   chan string
	done chan struct{}
}

type queue struct {
	mu      sync.Mutex
	msgs    []string
	waiters []*waiter
}

var (
	mu     sync.Mutex
	queues = map[string]*queue{}
)

func getQueue(name string) *queue {
	mu.Lock()
	defer mu.Unlock()
	if queues[name] == nil {
		queues[name] = new(queue)
	}
	return queues[name]
}

func (q *queue) put(s string) {
	q.mu.Lock()
	for len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		q.mu.Unlock()
		select {
		case w.ch <- s:
			return
		case <-w.done:
		}
		q.mu.Lock()
	}
	q.msgs = append(q.msgs, s)
	q.mu.Unlock()
}

func (q *queue) get(timeout int) (string, bool) {
	q.mu.Lock()
	if len(q.msgs) > 0 {
		s := q.msgs[0]
		q.msgs = q.msgs[1:]
		q.mu.Unlock()
		return s, true
	}
	if timeout == 0 {
		q.mu.Unlock()
		return "", false
	}
	w := &waiter{ch: make(chan string), done: make(chan struct{})}
	q.waiters = append(q.waiters, w)
	q.mu.Unlock()
	t := time.NewTimer(time.Duration(timeout) * time.Second)
	defer t.Stop()
	select {
	case s := <-w.ch:
		return s, true
	case <-t.C:
		q.mu.Lock()
		for i, x := range q.waiters {
			if x == w {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				close(w.done)
				break
			}
		}
		q.mu.Unlock()
		select {
		case s := <-w.ch:
			return s, true
		default:
			return "", false
		}
	}
}

func main() {
	port := flag.String("port", "8080", "")
	flag.Parse()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if len(p) < 2 || p[0] != '/' {
			w.WriteHeader(404)
			return
		}
		q := getQueue(p[1:])
		switch r.Method {
		case http.MethodPut:
			vals := r.URL.Query()
			vs, ok := vals["v"]
			if !ok {
				w.WriteHeader(400)
				return
			}
			q.put(vs[0])
		case http.MethodGet:
			timeout := 0
			if s := r.URL.Query().Get("timeout"); s != "" {
				if v, err := strconv.Atoi(s); err == nil && v > 0 {
					timeout = v
				}
			}
			if s, ok := q.get(timeout); !ok {
				w.WriteHeader(404)
				return
			} else {
				w.Write([]byte(s))
			}
		default:
			w.WriteHeader(405)
		}
	})
	panic(http.ListenAndServe(":"+*port, nil))
}
