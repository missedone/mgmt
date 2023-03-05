// Mgmt
// Copyright (C) 2013-2023+ James Shubin and the project contributors
// Written by James Shubin <james@shubin.ca> and the project contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package dage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

var ErrClosed = errors.New("already closed")

// myVertex is a vertex reference implementation which is used for documentation
// and for testing.
type myVertex struct {
	Name string

	init *Init

	wg *sync.WaitGroup

	eventsChan chan struct{}
	closedChan chan struct{}
	closed     bool
}

func (obj *myVertex) String() string {
	return fmt.Sprintf("myVertex: %s", obj.Name)
}

func (obj *myVertex) Init(init *Init) error {
	obj.init = init // save for later

	obj.wg = &sync.WaitGroup{} // XXX: unused for now

	obj.eventsChan = make(chan struct{})
	obj.closedChan = make(chan struct{})

	return nil
}

func (obj *myVertex) Close() error {
	//close(obj.eventsChan) // leave it so we don't send on a closed channel
	return nil
}

// XXX: think about the logic and how this might be able to be "Retried"... Will it work okay?
func (obj *myVertex) Run(ctx context.Context) error {
	defer obj.wg.Wait()
	defer close(obj.closedChan)

	startupChan := make(chan struct{})
	close(startupChan)
	event := func() {
		startupChan = nil // we get at most one startup event somewhere
		_ = obj.init.Event(ctx)
	}

	for {
		select {
		case <-startupChan:
			// cause the initial startup event to get sent!

		case <-obj.eventsChan:
			obj.init.Logf("%s", obj.String()+": recv event!")

		case <-ctx.Done(): // when asked to exit
			obj.closed = true
			return nil // we exit happily
		}

		// now decide to send an event...
		event()
	}
}

// Event is called by the engine to tell this vertex that it is receiving an
// event. It should error if it can't receive the event for some reason. It must
// always be safe to call even after this vertex has closed.
func (obj *myVertex) Event() error {
	select {
	case obj.eventsChan <- struct{}{}: // is this too simple a mechanism?
		if obj.closed {
			return ErrClosed
		}
		return nil

	case <-obj.closedChan:
		return ErrClosed
	}
}

type myEdge struct {
	Name string
}

func (obj *myEdge) String() string {
	return fmt.Sprintf("myEdge: %s", obj.Name)
}

func panicErr(err error) {
	if err != nil {
		panic(err)
	}
}

func TestDage1(t *testing.T) {
	//now := time.Now()
	min := 5 * time.Second // approx min time needed for the test
	ctx := context.Background()
	if deadline, ok := t.Deadline(); ok {
		d := deadline.Add(-min)
		//t.Logf("  now: %+v", now)
		//t.Logf("    d: %+v", d)
		newCtx, cancel := context.WithDeadline(ctx, d)
		ctx = newCtx
		defer cancel()
	}

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			t.Logf("cancelling test...")
		}
	}()

	v1 := &myVertex{Name: "v1"}
	v2 := &myVertex{Name: "v2"}
	e1 := &myEdge{Name: "e1"}

	engine := &Engine{
		Name: "dage",

		Debug: testing.Verbose(), // set via the -test.v flag to `go test`
		Logf:  t.Logf,
	}

	if err := engine.Setup(); err != nil {
		t.Errorf("could not setup engine: %+v", err)
		return
	}
	defer engine.Cleanup()

	//fn := func(graph *pgraph.Graph) error {
	//	//graph.AddVertex(...)
	//	return nil
	//}

	// XXX: can/should we lock/unlock before we do the Run() ? Or maybe
	// they're just not necessary?
	//engine.Lock()
	//engine.Op(fn) // Could do multiple graph op's in one mutex?

	panicErr(engine.AddVertex(v1))
	panicErr(engine.AddVertex(v2))
	panicErr(engine.AddEdge(v1, v2, e1))

	//engine.Unlock()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := engine.Run(ctx); err != nil {
			t.Errorf("error while running engine: %+v", err)
			return
		}
		t.Logf("engine shutdown cleanly...")
	}()

	v3 := &myVertex{Name: "v3"}
	v4 := &myVertex{Name: "v4"}
	e2 := &myEdge{Name: "e2"}
	e3 := &myEdge{Name: "e3"}

	engine.Lock()
	//engine.Op(fn) // Could do multiple graph op's in one mutex?

	panicErr(engine.AddVertex(v3))
	panicErr(engine.AddVertex(v4))
	panicErr(engine.AddEdge(v3, v4, e3))
	panicErr(engine.AddEdge(v2, v3, e2)) // connect the first and second chunks

	engine.Unlock()

	time.Sleep(3 * time.Second)
}
