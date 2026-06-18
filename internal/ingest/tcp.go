package ingest

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type Server struct {
	addr         string
	frameSize    int
	listener     net.Listener
	onFrame      func([]byte)
	mu           sync.Mutex
	connections  map[net.Conn]struct{}
	done         chan struct{}
	wg           sync.WaitGroup
	readTimeout  time.Duration
	maxConns     int
}

type ServerOption func(*Server)

func WithReadTimeout(d time.Duration) ServerOption {
	return func(s *Server) { s.readTimeout = d }
}

func WithMaxConnections(n int) ServerOption {
	return func(s *Server) { s.maxConns = n }
}

func NewServer(addr string, frameSize int, handler func([]byte), opts ...ServerOption) *Server {
	s := &Server{
		addr:        addr,
		frameSize:   frameSize,
		onFrame:     handler,
		connections: make(map[net.Conn]struct{}),
		done:        make(chan struct{}),
		maxConns:    100,
		readTimeout: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Server) Start() error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(nil, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("ingest: listen failed: %w", err)
	}
	s.listener = ln

	s.wg.Add(1)
	go s.acceptLoop()

	log.Printf("[ingest] TCP server listening on %s", s.addr)
	return nil
}

func (s *Server) Stop() {
	close(s.done)
	if s.listener != nil {
		s.listener.Close()
	}
	s.mu.Lock()
	for conn := range s.connections {
		conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	log.Printf("[ingest] TCP server stopped")
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				log.Printf("[ingest] accept error: %v", err)
				continue
			}
		}

		s.mu.Lock()
		if len(s.connections) >= s.maxConns {
			s.mu.Unlock()
			log.Printf("[ingest] max connections reached, rejecting %s", conn.RemoteAddr())
			conn.Close()
			continue
		}
		s.connections[conn] = struct{}{}
		s.mu.Unlock()

		log.Printf("[ingest] new connection from %s", conn.RemoteAddr())
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.connections, conn)
		s.mu.Unlock()
		s.wg.Done()
		log.Printf("[ingest] connection closed %s", conn.RemoteAddr())
	}()

	if s.readTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(s.readTimeout))
	}

	buf := make([]byte, 4096)
	for {
		select {
		case <-s.done:
			return
		default:
		}

		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				conn.SetReadDeadline(time.Now().Add(s.readTimeout))
				continue
			}
			log.Printf("[ingest] read error from %s: %v", conn.RemoteAddr(), err)
			return
		}

		if n > 0 && s.onFrame != nil {
			data := make([]byte, n)
			copy(data, buf[:n])
			s.onFrame(data)
		}

		if s.readTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(s.readTimeout))
		}
	}
}

func (s *Server) ConnectionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.connections)
}
