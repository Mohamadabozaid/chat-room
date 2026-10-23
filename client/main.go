package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"chatroom/common"
)

const pollEvery = 500 * time.Millisecond

var (
	outMu    sync.Mutex
	username string
)

// printLine prints above the prompt so incoming messages don't garble typing.
func printLine(s string) {
	outMu.Lock()
	defer outMu.Unlock()
	fmt.Printf("\r\033[K%s\n> ", s)
}

func render(m common.Message) string {
	ts := m.Time.Format("15:04:05")
	switch m.Kind {
	case common.KindChat:
		return fmt.Sprintf("[%s] %s: %s", ts, m.From, m.Text)
	default:
		return fmt.Sprintf("[%s] *** %s ***", ts, m.Text)
	}
}

func main() {
	addr := flag.String("addr", "localhost:1234", "server address")
	name := flag.String("name", "", "username (prompted if empty)")
	flag.Parse()

	client, err := rpc.Dial("tcp", *addr)
	if err != nil {
		fmt.Println("cannot connect to server:", err)
		os.Exit(1)
	}
	defer client.Close()

	stdin := bufio.NewReader(os.Stdin)
	username = strings.TrimSpace(*name)

	// Join loop: keep asking until the username is accepted.
	for {
		if username == "" {
			fmt.Print("Enter username: ")
			line, err := stdin.ReadString('\n')
			if err != nil {
				return
			}
			username = strings.TrimSpace(line)
		}
		err := client.Call("Chat.Join", common.JoinArgs{Username: username}, &common.Empty{})
		if err == nil {
			break
		}
		fmt.Println("join failed:", err)
		username = ""
	}
	fmt.Printf("Welcome, %s! Commands: /users  /quit  (Ctrl+C also leaves cleanly)\n", username)

	var leaveOnce sync.Once
	leave := func() {
		leaveOnce.Do(func() {
			client.Call("Chat.Leave", common.LeaveArgs{Username: username}, &common.Empty{})
		})
	}

	// Ctrl+C / SIGTERM => leave cleanly.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		leave()
		fmt.Println("\nGoodbye!")
		os.Exit(0)
	}()

	// Polling goroutine: fetch pending messages automatically.
	go func() {
		t := time.NewTicker(pollEvery)
		defer t.Stop()
		for range t.C {
			var reply common.PollReply
			if err := client.Call("Chat.Poll", common.PollArgs{Username: username}, &reply); err != nil {
				fmt.Println("\nconnection to server lost:", err)
				os.Exit(1)
			}
			for _, m := range reply.Messages {
				printLine(render(m))
				if m.Kind == common.KindShutdown {
					fmt.Println("\nServer closed. Exiting.")
					os.Exit(0)
				}
			}
		}
	}()

	// Input loop.
	for {
		fmt.Print("> ")
		line, err := stdin.ReadString('\n') // EOF (Ctrl+D) => leave
		if err != nil {
			leave()
			return
		}
		line = strings.TrimSpace(line)
		switch {
		case line == "":
		case line == "/quit":
			leave()
			fmt.Println("Goodbye!")
			return
		case line == "/users":
			var r common.ListReply
			if err := client.Call("Chat.ListUsers", common.Empty{}, &r); err != nil {
				printLine("error: " + err.Error())
				continue
			}
			printLine("Online: " + strings.Join(r.Users, ", "))
		default:
			if err := client.Call("Chat.Send", common.SendArgs{Username: username, Text: line}, &common.Empty{}); err != nil {
				printLine("send failed: " + err.Error())
			}
		}
	}
}
