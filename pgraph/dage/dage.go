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

	Debug bool
	Logf  func(format string, v ...interface{})

	graph *pgraph.Graph
	state map[Vertex]*state
	waits map[Vertex]*sync.WaitGroup // wg for the vertex Run func

	// mutex wraps any internal operation so that this library is
	// thread-safe. It especially guards access to graph and state fields.
	mutex *sync.Mutex
	wg    *sync.WaitGroup

	// pause/resume state machine signals
	pauseChan   chan struct{}
	pausedChan  chan struct{}
	resumeChan  chan struct{}
	resumedChan chan struct{}
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
	obj.wg = &sync.WaitGroup{}

	obj.pauseChan = make(chan struct{})
	obj.pausedChan = make(chan struct{})
	obj.resumeChan = make(chan struct{})
	obj.resumedChan = make(chan struct{})

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

	obj.state[v] = &state{
		running:    false,
		eventsChan: make(chan struct{}),
		wg:         &sync.WaitGroup{},
	}

	obj.graph.AddVertex(v)

	// TODO: can we batch graph operations so that we only have to do this
	// once per Lock/Unlock cycle?
	topologicalSort, err := obj.graph.TopologicalSort()
	if err != nil {
		return err // not a dag!
	}
	_ = topologicalSort

	init := &Init{
		// The vertex calls this function to tell us it wants to send an
		// event to the children/descendent vertices!
		Event: func(ctx context.Context) error {
			select {
			case obj.state[v].eventsChan <- struct{}{}:
				obj.Logf("%s", v.String()+": sent event!")
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
		return err // not a dag!
	}
	_ = topologicalSort

	return nil
}

func (obj *Engine) DeleteVertex(v Vertex) error {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()

	if _, exists := obj.state[v]; !exists {
		return fmt.Errorf("vertex %s doesn't exist", v)
	}

	if obj.state[v].running {
		// cancel the running vertex
		obj.state[v].cancelCtx()
		obj.state[v].wg.Wait()
	}

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
	select {
	case obj.pauseChan <- struct{}{}:
	}
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
	// waiting for the resume to move to resumed...
	select {
	case <-obj.resumedChan:
	}
}

func (obj *Engine) Run(ctx context.Context) error {
	obj.mutex.Lock()
	defer obj.mutex.Unlock()
	obj.wg.Add(1)
	defer obj.wg.Done()
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
		// could then immediately change.
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

			if obj.state[v].running { // it's not a new vertex
				continue
			}
			obj.state[v].running = true

			// run mainloop
			obj.state[v].wg.Add(1)
			go func(v Vertex) {
				defer obj.state[v].wg.Done()
				defer close(obj.state[v].eventsChan)
				ctx, cancel := context.WithCancel(ctx) // wrap parent
				obj.state[v].ctx = ctx
				obj.state[v].cancelCtx = cancel
				runErr := v.Run(obj.state[v].ctx)
				if runErr != nil {
					// XXX: send to a channel
				}

			}(v)

			// process events
			obj.state[v].wg.Add(1)
			go func(v Vertex) {
				defer obj.state[v].wg.Done()
				for {
					select {
					case _, ok := <-obj.state[v].eventsChan:
						if !ok { // no more events
							return
						}
					}

					//XXX: we need to maybe add a mutex here because we don't want to be adding a new vertex here but then missing to send an event to it because it started after we did the range...
					//XXX: or maybe the mutex goes above the select and also blocks events???
					//XXX: maybe an RWLock?
					//XXX:					obj.mutex.Rlock()

					// XXX: cache/memoize this value
					outgoing := obj.graph.OutgoingGraphVertices(v) // []Vertex
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
				}
			}(v)
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

// state tracks some internal vertex-specific state information.
type state struct {
	running    bool
	eventsChan chan struct{}
	wg         *sync.WaitGroup
	ctx        context.Context
	cancelCtx  func()
}
