// Copyright IBM Corp. 2015, 2026
// SPDX-License-Identifier: MPL-2.0

package memdb

//go:generate sh -c "cd .. && go run ./watch-gen | gofmt >watch_few.go"

import (
	"context"
	"time"
)

// aFew gives how many watchers this function is wired to support. You must
// always pass a full slice of this length, but unused channels can be nil.
const aFew = 32

// watchFew is used if there are only a few watchers as a performance
// optimization.
func watchFew(ctx context.Context, ch []<-chan struct{}) error {
	select {

	case <-ch[0]:
		return nil

	case <-ch[1]:
		return nil

	case <-ch[2]:
		return nil

	case <-ch[3]:
		return nil

	case <-ch[4]:
		return nil

	case <-ch[5]:
		return nil

	case <-ch[6]:
		return nil

	case <-ch[7]:
		return nil

	case <-ch[8]:
		return nil

	case <-ch[9]:
		return nil

	case <-ch[10]:
		return nil

	case <-ch[11]:
		return nil

	case <-ch[12]:
		return nil

	case <-ch[13]:
		return nil

	case <-ch[14]:
		return nil

	case <-ch[15]:
		return nil

	case <-ch[16]:
		return nil

	case <-ch[17]:
		return nil

	case <-ch[18]:
		return nil

	case <-ch[19]:
		return nil

	case <-ch[20]:
		return nil

	case <-ch[21]:
		return nil

	case <-ch[22]:
		return nil

	case <-ch[23]:
		return nil

	case <-ch[24]:
		return nil

	case <-ch[25]:
		return nil

	case <-ch[26]:
		return nil

	case <-ch[27]:
		return nil

	case <-ch[28]:
		return nil

	case <-ch[29]:
		return nil

	case <-ch[30]:
		return nil

	case <-ch[31]:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

// watchFewTimer is the timer-based counterpart of watchFew, used by Watch for
// watch sets with at most aFew channels. Folding timeoutCh into the same
// select lets Watch block in the caller's goroutine instead of spawning a
// goroutine just to bridge the timer into a context. A nil timeoutCh blocks
// forever, matching a receive on a nil channel.
func watchFewTimer(ctx context.Context, ch []<-chan struct{}, timeoutCh <-chan time.Time) error {
	select {

	case <-ch[0]:
		return nil

	case <-ch[1]:
		return nil

	case <-ch[2]:
		return nil

	case <-ch[3]:
		return nil

	case <-ch[4]:
		return nil

	case <-ch[5]:
		return nil

	case <-ch[6]:
		return nil

	case <-ch[7]:
		return nil

	case <-ch[8]:
		return nil

	case <-ch[9]:
		return nil

	case <-ch[10]:
		return nil

	case <-ch[11]:
		return nil

	case <-ch[12]:
		return nil

	case <-ch[13]:
		return nil

	case <-ch[14]:
		return nil

	case <-ch[15]:
		return nil

	case <-ch[16]:
		return nil

	case <-ch[17]:
		return nil

	case <-ch[18]:
		return nil

	case <-ch[19]:
		return nil

	case <-ch[20]:
		return nil

	case <-ch[21]:
		return nil

	case <-ch[22]:
		return nil

	case <-ch[23]:
		return nil

	case <-ch[24]:
		return nil

	case <-ch[25]:
		return nil

	case <-ch[26]:
		return nil

	case <-ch[27]:
		return nil

	case <-ch[28]:
		return nil

	case <-ch[29]:
		return nil

	case <-ch[30]:
		return nil

	case <-ch[31]:
		return nil

	case <-timeoutCh:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	}
}
