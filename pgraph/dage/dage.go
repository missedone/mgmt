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

// Package dage implements a DAG engine.
package dage

import (
	"context"
	"fmt"
	"sync"

	"github.com/purpleidea/mgmt/pgraph"
)

// Init is a structure of useful values and handles which is passed into all
// vertices on initialization. It should use it to communicate with the outside
// world. It should keep a handle to this struct for future use.
type Init struct {
	//XXX	Ping func()

	// Event is used to tell the engine that a descendent vertex has new
	// data that it should look for. It returns an error if it unblocked
	// because we are shutting down early for some reason and possibly
	// didn't send the actual event. It must not be called after the Run
	// method of the Vertex has exited. In other words, have it be run
	// synchronously in the Run mainloop and never in a goroutine that could
	// get accidentally called after Run has exited.
	Event func(context.Context) error

	Debug bool
	Logf  func(format string, v ...interface{})
}

// Vertex is the primary vertex struct in this library. It can be anything that
// implements its interface. The string output must be stable and unique in the
// graph.
type Vertex interface {
	fmt.Stringer // String() string

	// Init is called once for each vertex at first startup. It provides
	// some useful handles to the vertex for future use.
	Init(*Init) error // only once at start

	// Close is called once at the very end for clean up. It is guaranteed
	// to be called as long as Init was called successfully or unless the
	// whole engine panics.
	Close() error // only once at end

	// Run starts up whatever internal routines this vertex needs for its
	// lifecycle. This method should block until it shuts down forever. It
	// will send an initial notification via Event() unblocking once at
	// startup when it is ready, and may choose to send additional
	// notifications via the same mechanism if it wishes to notify
	// downstream vertices. You cancel the input context to get the whole
	// thing to shutdown. It only returns an error if something has gone
	// wrong during its execution.
	Run(context.Context) error

	// XXX: Considering this API for now...
	// Event is called to send a message to this vertex that something has
	// evented in an antecedent vertex. The engine calls this method when
	// needed. It must be always safe to call this method any time after
	// Init and even if Run has shutdown and Close has been run. This is to
	// avoid having to worry about a slow/delayed message coming in late. It
	// should also not block forever and we generally want a fast reply when
	// possible.
	// XXX: It should return an error if the notification can't be received
	// at this time, which should only happen because it isn't running
	// anymore. (XXX: Should we allow it to support such a behaviour?)
	// TODO: should Event have an input arg of vertices that evented?
	// TODO: should Event have an input ctx so we can cancel it early? -- Does it piggyback on the Run ctx?
	Event() error

	// XXX: define the API that vertices should implement.
	// XXX: what kind of API do we want for talking to and from them?
	// XXX: is any of the below even necessary?
	//Pause(XXX) error // to pause
	//Resume(XXX) error // to resume
}

type RestartableVertex interface {
	Vertex

	// Reset must be called after Run exits, but before it is restarted by
	// the engine. If the Run method can be safely, immediately run right
	// away, then this can be a no-op. Keep in mind that when Run is started
	// for the subsequent time, it must still generate an initial startup
	// event via the Event unblocking mechanism.
	Reset() error
}

// Edge is the primary edge struct in this library. It can be anything that
// implements its interface. The string output must be stable and unique in the
// graph.
type Edge interface {
	fmt.Stringer // String() string
}

// Engine implements a dag engine which lets us "run" a dag but also allows us
// to modify it while we are running. XXX
// XXX: this could be wrapped with the Txn API we wrote...
type Engine struct {
	// Name is the name used for the instance of the engine and in the graph
	// that is held within it.
	Name string

	// Glitch: https://en.wikipedia.org/wiki/Reactive_programming#Glitches
	Glitch bool // allow glitching? (more responsive, but less accurate)

	// Callback can be specified as an alternative to using the Stream
	// method to get events. If the context on it is cancelled, then it must
	// shutdown quickly, because this means we are closing and want to
	// disconnect. Whether you want to respect that is up to you, but the
	// engine will not be able to close until you do.
	Callback func(context.Context)

	Debug bool
	Logf  func(format string, v ...interface{})

	graph *pgraph.Graph
	state map[Vertex]*state

	// mutex wraps any internal operation so that this library is
	// thread-safe. It especially guards access to graph and state fields.
	mutex   *sync.Mutex
	rwmutex *sync.RWMutex
	wg      *sync.WaitGroup

	// pause/resume state machine signals
	pauseChan   chan struct{}
	pausedChan  chan struct{}
	resumeChan  chan struct{}
	resumedChan chan struct{}

	ag      chan error // used to aggregate events
	agwg *sync.WaitGroup
	streamChan chan error

}

// Setup sets up the internal datastructures needed for this engine.
// XXX: after Setup() it is considered to be "running", but of course you need to
// first Lock then Add/Remove/etc and then Unlock since an empty graph "running"
// doesn't do much good.
func (obj *Engine) Setup() error {
	var err error
	obj.graph, err = pgraph.NewGraph(obj.Name)
	if err != nil {
		return err
	}
	obj.state = make(map[Vertex]*state)
	obj.mutex = &sync.Mutex{}
	obj.rwmutex = &sync.RWMutex{}
	obj.wg = &sync.WaitGroup{}

	obj.pauseChan = make(chan struct{})
	obj.pausedChan = make(chan struct{})
	obj.resumeChan = make(chan struct{})
	obj.resumedChan = make(chan struct{})

	obj.ag = make(chan error)
	obj.agwg = &sync.WaitGroup{}
	obj.streamChan = make(chan error)

	return nil
}

// Cleanup cleans up and frees memory and resources after everything is done.
func (obj *Engine) Cleanup() error {
	obj.wg.Wait()        // don't cleanup these before Run() finished
	close(obj.pauseChan) // free
	close(obj.pausedChan)
	close(obj.resumeChan)
	close(obj.resumedChan)
	return nil
}

// addVertex is the lockless version of the AddVertex function. This is needed
// so that AddEdge can add two vertices within the same lock.
func (obj *Engine) addVertex(v Vertex) error {

	if _, exists := obj.state[v]; exists {
		// don't err dupes, because it makes using the AddEdge API yucky
		return nil
	}

	// This is the one of two places where we modify this map. To avoid
	// concurrent writes, we only do this when we're locked! Anywhere that
	// can read where we are locked must have a mutex around it or do the
	// lookup when we're in an unlocked state.
	n := &state{
		running:    false,
		eventsChan: make(chan struct{}),
		wg:         &sync.WaitGroup{},
	}
	obj.state[v] = n // n for node

	obj.graph.AddVertex(v)

	// TODO: can we batch graph operations so that we only have to do this
	// once per Lock/Unlock cycle?
	topologicalSort, err := obj.graph.TopologicalSort()
	if err != nil {
		return err // not a dag
	}
	_ = topologicalSort

	init := &Init{
		// The vertex calls this function to tell us it wants to send an
		// event to the children/descendent vertices!
		Event: func(ctx context.Context) error {
			select {
			case n.eventsChan <- struct{}{}:
				return nil

			case <-ctx.Done():
				return fmt.Errorf("cancelled")
			}
		},
		Debug: obj.Debug,
		Logf: func(format string, vs ...interface{}) {
			// safe Logf in case v.String contains %? chars...
			s := v.String() + ": " + fmt.Sprintf(format, vs...)
			obj.Logf("%s", s)
		},
	}

	return v.Init(init)
}

// XXX: add internal operations lock around modifications? (to make this code thread safe)
func (obj *Engine) AddVertex(v Vertex) error {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	return obj.addVertex(v) // lockless version
}

func (obj *Engine) AddEdge(v1, v2 Vertex, e Edge) error {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	if err := obj.addVertex(v1); err != nil { // lockless version
		return err
	}
	if err := obj.addVertex(v2); err != nil {
		return err
	}
	obj.graph.AddEdge(v1, v2, e)

	topologicalSort, err := obj.graph.TopologicalSort()
	if err != nil {
		return err // not a dag
	}
	_ = topologicalSort

	return nil
}

func (obj *Engine) DeleteVertex(v Vertex) error {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	n, exists := obj.state[v]
	if !exists {
		return fmt.Errorf("vertex %s doesn't exist", v)
	}

	if n.running {
		// cancel the running vertex
		n.cancelCtx()
		n.wg.Wait()
	}

	// This is the one of two places where we modify this map. To avoid
	// concurrent writes, we only do this when we're locked! Anywhere that
	// can read where we are locked must have a mutex around it or do the
	// lookup when we're in an unlocked state.
	delete(obj.state, v)
	obj.graph.DeleteVertex(v)
	return v.Close()
}

func (obj *Engine) DeleteEdge(e Edge) error {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	// Don't bother checking if edge exists first and don't error if it
	// doesn't because it might have gotten deleted when a vertex did, and
	// so there's no need to complain for nothing.
	obj.graph.DeleteEdge(e)

	return nil
}

// Lock must be used before modifying the running graph. Make sure to Unlock
// when done.
func (obj *Engine) Lock() { // pause
	obj.rwmutex.Lock() // XXX: or should it go right after pauseChan?
	select {
	case obj.pauseChan <- struct{}{}:
	}
	//obj.rwmutex.Lock() // XXX: or should it go right before pauseChan?

	// waiting for the pause to move to paused...
	select {
	case <-obj.pausedChan:
	}
	// this mutex locks at start of Run() and unlocks at finish of Run()
	obj.mutex.Unlock() // safe to make changes now
}

// Unlock must be used after modifying the running graph. Make sure to Lock
// beforehand.
func (obj *Engine) Unlock() { // resume
	// this mutex locks at start of Run() and unlocks at finish of Run()
	obj.mutex.Lock() // no more changes are allowed
	select {
	case obj.resumeChan <- struct{}{}:
	}
	//obj.rwmutex.Unlock() // XXX: or should it go right after resumedChan?

	// waiting for the resume to move to resumed...
	select {
	case <-obj.resumedChan:
	}
	obj.rwmutex.Unlock() // XXX: or should it go right before resumedChan?
}

// Run kicks off the main engine. This takes a mutex. When we're "paused" the
// mutex is temporarily released until we "resume". Those operations transition
// with the engine Lock and Unlock methods. It is recommended to only add
// vertices to the engine after it's running. If you add them before Run, then
// Run will cause a Lock/Unlock to occur to cycle them in. Lock and Unlock race
// with the cancellation of this Run main loop. Make sure to only call one at a
// time.
func (obj *Engine) Run(ctx context.Context) error {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()
	obj.wg.Add(1)
	defer obj.wg.Done()
	ctx, cancel := context.WithCancel(ctx) // wrap parent
	defer cancel()

	if n := obj.graph.NumVertices(); n > 0 { // hack to make the api easier
		obj.Logf("graph contained %d vertices before Run", n)
		obj.wg.Add(1)
		go func() {
			defer obj.wg.Done()
			// kick the engine once to pull in any vertices from
			// before we started running!
			defer obj.Unlock()
			obj.Lock()
		}()
	}

	// close the aggregate channel when everyone is done with it...
	obj.wg.Add(1)
	go func() {
		defer obj.wg.Done()
		select {
		case <-ctx.Done():
		}
		// don't wait and close ag before we're really done with Run()
		obj.agwg.Wait() // wait for last ag user to close
		close(obj.ag)   // last one closes the ag channel
	}()

	// aggregate events channel
	obj.wg.Add(1)
	go func() {
		defer obj.wg.Done()
		defer close(obj.streamChan)
		for {
			var err error
			var ok bool
			select {
			case err, ok = <-obj.ag: // aggregated channel
				if !ok {
					return // channel shutdown
				}
			}

			// now send event...
			if obj.Callback != nil {
				// send stream signal (callback variant)
				obj.Callback(ctx)
			} else {
				// send stream signal
				select {
				// send events or errors on streamChan
				case obj.streamChan <- err: // send
					if err != nil {
						cancel() // cancel the context!
						return
					}
				case <-ctx.Done(): // when asked to exit
					return
				}
			}

		}
	}()

	// we start off "running", but we'll have an empty graph initially...
	for {

		// wait until paused/locked request comes in...
		select {
		case <-obj.pauseChan:
			obj.Logf("pausing...")

		case <-ctx.Done(): // when asked to exit
			return nil // we exit happily
		}

		// Toposort for paused workers. We run this before the actual
		// pause completes, because the second we are paused, the graph
		// could then immediately change. We don't need a lock in here
		// because the mutex only unlocks when pause is complete below.
		topoSort1, err := obj.graph.TopologicalSort()
		if err != nil {
			return err
		}
		for _, v := range topoSort1 {
			// XXX API
			//v.Pause?()
			//obj.state[vertex].whatever
			_ = v // XXX
		}

		// pause is complete
		// no exit case from here, must be fully running or paused...
		select {
		case obj.pausedChan <- struct{}{}:
			obj.Logf("paused!")
		}

		//
		// the graph changes shape right here... we are locked right now
		//

		// wait until resumed/unlocked
		select {
		case <-obj.resumeChan:
			obj.Logf("resuming...")

		case <-ctx.Done(): // when asked to exit
			return nil // we exit happily
		}

		// Toposort to run/resume workers. (Bottom of toposort first!)
		topoSort2, err := obj.graph.TopologicalSort()
		if err != nil {
			return err
		}
		reversed := pgraph.Reverse(topoSort2)
		for _, v := range reversed {
			v, ok := v.(Vertex)
			if !ok {
				panic("not a vertex")
			}
			n := obj.state[v] // n for node

			if n.running { // it's not a new vertex
				continue
			}
			n.running = true

			// run mainloop
			n.wg.Add(1)
			obj.agwg.Add(1)
			go func(v Vertex, n *state) {
				defer n.wg.Done()
				defer close(n.eventsChan)
				defer obj.agwg.Done()
				ctx, cancel := context.WithCancel(ctx) // wrap parent
				n.ctx = ctx
				n.cancelCtx = cancel
				runErr := v.Run(n.ctx)
				if runErr != nil {
					// send to a aggregate channel
					// the first to error will cause ag to
					// shutdown, so make sure we can exit...
					select {
					case obj.ag <- runErr: // send to aggregate channel
					case <-ctx.Done():
					}
				}
				// XXX: if node never loaded, then we error!

			}(v, n)

			// process events
			n.wg.Add(1)
			obj.agwg.Add(1)
			go func(v Vertex, n *state) {
				defer n.wg.Done()
				defer obj.agwg.Done()
				for {
					// <-eventsChan is a map race if we did
					// the lookup here. So instead, we do a
					// n := obj.state[v] and use that here.
					select {
					case _, ok := <-n.eventsChan:
						if !ok { // no more events
							return
						}
					}

					// XXX: maybe the rwmutex goes above the select and also blocks events???

					// XXX: I think we need this read lock
					// because we don't want to be adding a
					// new vertex here but then missing to
					// send an event to it because it
					// started after we did the range...
					obj.rwmutex.RLock()
					// XXX: cache/memoize this value
					outgoing := obj.graph.OutgoingGraphVertices(v) // []Vertex
					obj.rwmutex.RUnlock()
					for _, xv := range outgoing {
						xv, ok := xv.(Vertex)
						if !ok {
							panic("not a vertex")
						}

						// send event to vertex
						// It must be always safe (by
						// API) to call this method, but
						// while not ideal, even if it
						// shutdown, it should not block
						// forever or panic.
						if err := xv.Event(); err != nil {
							// couldn't receive!
							// TODO: should we do anything?
						}
					}

					if obj.Glitch || len(outgoing) == 0 {
						select {
						case obj.ag <- nil: // send to aggregate channel
						case <-ctx.Done():
							// let eventsChan return
							//return
						}
					}

				}
			}(v, n)
		}
		// now check their states...
		for _, v := range reversed {
			v, ok := v.(Vertex)
			if !ok {
				panic("not a vertex")
			}
			_ = v
			//close(obj.state[v].startup) XXX once?

			// wait for startup XXX XXX XXX XXX
			//select {
			//case <-obj.state[v].startup:
			////case XXX close?:
			//}

			// XXX API
			//v.Resume?()
			//obj.state[vertex].whatever
		}

		// resume is complete
		// no exit case from here, must be fully running or paused...
		select {
		case obj.resumedChan <- struct{}{}:
			obj.Logf("resumed!")
		}

	} // end for
}


// XXX chan-> or ->chan ?
func (obj *Engine) Stream() chan error {
	return obj.streamChan
}

// state tracks some internal vertex-specific state information.
type state struct {
	running    bool
	eventsChan chan struct{}
	wg         *sync.WaitGroup
	ctx        context.Context
	cancelCtx  func()
}
