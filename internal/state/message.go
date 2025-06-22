package state

import "github.com/lazygophers/utils/cryptox"

func GenMessageId() string {
	return cryptox.UUID()
}
