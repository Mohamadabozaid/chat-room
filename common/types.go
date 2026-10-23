package common

import "time"

// Message kinds.
const (
	KindChat     = "chat"     // normal message from a user
	KindSystem   = "system"   // join / leave notifications
	KindShutdown = "shutdown" // server is going down
)

type Message struct {
	Kind string
	From string
	Text string
	Time time.Time
}

type Empty struct{}

type JoinArgs struct{ Username string }
type LeaveArgs struct{ Username string }
type SendArgs struct {
	Username string
	Text     string
}
type PollArgs struct{ Username string }
type PollReply struct{ Messages []Message }
type ListReply struct{ Users []string }
