package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"chatroom/common"
)

const (
	reapEvery   = 2 * time.Second
	userTimeout = 10 * time.Second // no poll for this long => client considered dead
)

type userState struct {
	queue    []common.Message
	lastSeen time.Time
}

// ChatService is the RPC receiver. All state is guarded by mu.
type ChatService struct {
	mu    sync.Mutex
	users map[string]*userState
}

func NewChatService() *ChatService {
	return &ChatService{users: make(map[string]*userState)}
}

// broadcastLocked queues m for everyone except `except`. Caller must hold mu.
func (s *ChatService) broadcastLocked(except string, m common.Message) {
	for name, u := range s.users {
		if name != except {
			u.queue = append(u.queue, m)
		}
	}
}

func system(text string) common.Message {
	return common.Message{Kind: common.KindSystem, Text: text, Time: time.Now()}
}

func (s *ChatService) Join(args common.JoinArgs, _ *common.Empty) error {
	name := strings.TrimSpace(args.Username)
	if name == "" || strings.ContainsAny(name, " \t\n") {
		return errors.New("username must be non-empty and contain no spaces")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.users[name]; exists {
		return fmt.Errorf("username %q is already taken", name)
	}
	s.users[name] = &userState{lastSeen: time.Now()}
	s.broadcastLocked(name, system(name+" joined the chat"))
	log.Printf("+ %s joined (%d online)", name, len(s.users))
	return nil
}

func (s *ChatService) removeUser(name, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[name]; !ok {
		return
	}
	delete(s.users, name)
	s.broadcastLocked(name, system(name+" "+reason))
	log.Printf("- %s %s (%d online)", name, reason, len(s.users))
}

func (s *ChatService) Leave(args common.LeaveArgs, _ *common.Empty) error {
	s.removeUser(args.Username, "left the chat")
	return nil
}

func (s *ChatService) Send(args common.SendArgs, _ *common.Empty) error {
	text := strings.TrimSpace(args.Text)
	if text == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[args.Username]
	if !ok {
		return errors.New("you are not joined")
	}
	u.lastSeen = time.Now()
	s.broadcastLocked(args.Username, common.Message{
		Kind: common.KindChat, From: args.Username, Text: text, Time: time.Now(),
	})
	return nil
}

// Poll returns (and clears) all pending messages for the user.
func (s *ChatService) Poll(args common.PollArgs, reply *common.PollReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[args.Username]
	if !ok {
		return errors.New("you are not joined")
	}
	u.lastSeen = time.Now()
	reply.Messages = u.queue
	u.queue = nil
	return nil
}

func (s *ChatService) ListUsers(_ common.Empty, reply *common.ListReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name := range s.users {
		reply.Users = append(reply.Users, name)
	}
	sort.Strings(reply.Users)
	return nil
}

// reaper removes clients that stopped polling (crash, network loss, kill -9).
func (s *ChatService) reaper(stop <-chan struct{}) {
	t := time.NewTicker(reapEvery)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			var dead []string
			s.mu.Lock()
			for name, u := range s.users {
				if time.Since(u.lastSeen) > userTimeout {
					dead = append(dead, name)
				}
			}
			s.mu.Unlock()
			for _, name := range dead {
				s.removeUser(name, "disconnected unexpectedly")
			}
		}
	}
}

func (s *ChatService) announceShutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.broadcastLocked("", common.Message{
		Kind: common.KindShutdown, Text: "server is shutting down", Time: time.Now(),
	})
}

func main() {
	addr := flag.String("addr", ":1234", "listen address")
	flag.Parse()

	svc := NewChatService()
	srv := rpc.NewServer()
	if err := srv.RegisterName("Chat", svc); err != nil {
		log.Fatal(err)
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("chat server listening on %s", ln.Addr())

	stop := make(chan struct{})
	go svc.reaper(stop)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go srv.ServeConn(conn)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	log.Println("shutting down...")
	svc.announceShutdown()
	time.Sleep(1200 * time.Millisecond) // let clients poll the shutdown notice
	close(stop)
	ln.Close()
	log.Println("bye")
}
