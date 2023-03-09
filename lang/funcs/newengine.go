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

package funcs

import (
	"context"
	"fmt"
	"sync"

	"github.com/purpleidea/mgmt/engine"
	"github.com/purpleidea/mgmt/lang/interfaces"
	"github.com/purpleidea/mgmt/lang/types"
	"github.com/purpleidea/mgmt/pgraph"
	"github.com/purpleidea/mgmt/pgraph/dage"
//	"github.com/purpleidea/mgmt/util/errwrap"
)

// XXX: Rename to Engine when this is ready...
type NewEngine struct {
	// Name is the name used for the instance of the engine and in the graph
	// that is held within it.
	Name string

	dage *dage.Engine

	Hostname string
	World    engine.World
	Debug    bool
	Logf     func(format string, v ...interface{})

	// Glitch: https://en.wikipedia.org/wiki/Reactive_programming#Glitches
	Glitch bool // allow glitching? (more responsive, but less accurate)

	rwmutex *sync.RWMutex // use this rwmutex for touching table
	// XXX: what locks access to this?
	states map[interfaces.Func]*stateVertex // state associated with the vertex
	table map[interfaces.Func]types.Value
}

func (obj *NewEngine) Setup() error {

	obj.rwmutex = &sync.RWMutex{}
	obj.states = make(map[interfaces.Func]*stateVertex)
	obj.table = make(map[interfaces.Func]types.Value)

	obj.dage = &dage.Engine{
		Name: obj.Name,
		Debug: obj.Debug,
		Logf: obj.Logf,
		Glitch: obj.Glitch,
	}
	return obj.dage.Setup()
}

func (obj *NewEngine) Cleanup() error {
	return obj.dage.Cleanup()
}


//func (obj *NewEngine) AddVertex(v Vertex) error {
func (obj *NewEngine) AddVertex(f interfaces.Func) error {

	input := make(chan types.Value)
	output := make(chan types.Value)

	vertex := &stateVertex{
		Name: f.String(),
		Func: f,

		Input: input,
		Output: output,
		States: obj.states,
		Graph: obj.dage.Graph(), // XXX: what mutex guards access to this?
		Table: obj.table, // the engine's table
		Mutex: obj.rwmutex,

		Hostname: obj.Hostname,
		World: obj.World,

		// These two come in through *dage.Init, so no need to pass in.
		//Debug: obj.Debug,
		//Logf: func(format string, vs ...interface{}) {
		//	// safe Logf in case f.String contains %? chars...
		//	s := f.String() + ": " + fmt.Sprintf(format, vs...)
		//	obj.Logf("%s", s)
		//},
	}
	obj.states[f] = vertex // XXX ADD a MUTEX!

	// this will cause stateVertex.Init() to run
	return obj.dage.AddVertex(vertex)
}


func (obj *NewEngine) Lock() {
	obj.dage.Lock()


}

func (obj *NewEngine) Unlock() {
	obj.dage.Unlock()
}




func (obj *NewEngine) Run(ctx context.Context) error {



	err := obj.dage.Run(ctx)

	return err
}



// XXX chan-> or ->chan ?
func (obj *NewEngine) Stream() chan error {
	return obj.dage.Stream()
}

// XXX: temporary API for testing... Need to add read locks around this...
// XXX: and maybe we change the API to automatically RLock()/RUnlock() and return a single value or a range of values.
func (obj *NewEngine) Table() map[interfaces.Func]types.Value {
	return obj.table
}


type stateVertex struct {
	Name string
	Func interfaces.Func

	Hostname string
	World    engine.World

	// These two come in through *dage.Init, so no need to pass in.
	//Debug bool
	//Logf  func(format string, v ...interface{})

	Input  chan types.Value
	Output chan types.Value
	States map[interfaces.Func]*stateVertex
	Graph  *pgraph.Graph
	Table  map[interfaces.Func]types.Value
	Mutex  *sync.RWMutex

	// init contains some special fields from dage
	init *dage.Init

	wg *sync.WaitGroup

	eventsChan chan struct{}
	closedChan chan struct{}
}

func (obj *stateVertex) String() string {
	return fmt.Sprintf("stateVertex: %s", obj.Name)
}

func (obj *stateVertex) Init(init *dage.Init) error {
	obj.init = init // save for later

	funcInit := &interfaces.Init{
		Hostname: obj.Hostname,
		Input:    obj.Input,
		Output:   obj.Output,
		World:    obj.World,
		Debug:    obj.init.Debug,
		Logf: func(format string, v ...interface{}) {
			obj.init.Logf("func: "+format, v...)
		},
	}

	if err := obj.Func.Init(funcInit); err != nil {
		return err
	}

	obj.wg = &sync.WaitGroup{} // XXX: unused for now

	obj.eventsChan = make(chan struct{})
	obj.closedChan = make(chan struct{})

	return nil
}

func (obj *stateVertex) Close() error {
	//close(obj.eventsChan) // leave it so we don't send on a closed channel
	return nil
}

// XXX: think about the logic and how this might be able to be "Retried"... Will it work okay?
func (obj *stateVertex) Run(ctx context.Context) error {
	defer obj.wg.Wait()
	defer close(obj.closedChan) // XXX: replace with a ctx closer so that if we ran Close() but never Run then we'd be okay?

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // build a cancel for ourselves

	once := &sync.Once{}
	closeInput := func() { close(obj.Input) }
	defer once.Do(closeInput)



	// Receive from the function's Stream method.
	streamChan := make(chan error)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(streamChan)
		defer cancel() // make sure we unblock anyone if we close first!
		// XXX: should we have a retry mechanism here? Or in the parent Run?
		//err := obj.Func.Stream(ctx) // XXX: new API!
		err := obj.Func.Stream()
		select {
		case streamChan <- err:
		}
	}()
	// XXX: replace with ctx closer in new API.
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done(): // wait for main shutdown signal
		}
		_ = obj.Func.Close()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// XXX: do we need to synchronize this sending with the graph changes?
		// XXX: what lock do we use the guard access here?
		incoming := obj.Graph.IncomingGraphVertices(obj.Func) // []Vertex
		if len(incoming) != 0 {
			return
		}
		select {
		case obj.eventsChan<-struct{}{}: // startup the eventsChan...
		case <-ctx.Done(): // unblock
		}

	}()



	// XXX: get the initial startup from our child Stream instead!
	//startupChan := make(chan struct{})
	//close(startupChan)
	//event := func() {
	//	startupChan = nil // we get at most one startup event somewhere
	//	if err := obj.init.Event(ctx); err != nil {
	//		return
	//	}
	//	obj.init.Logf("sent event")
	//}

	for {
		select {
		//case <-startupChan:
		//	// cause the initial startup event to get sent!

		case err, ok := <-streamChan: // exit this loop only from here!
			obj.init.Logf("stream event: %+v", err)
			if !ok {
				return nil
			}
			return err

		case <-obj.eventsChan:
			obj.init.Logf("recv event")
			// XXX: memoize/cache this lookup
			// XXX: what lock do we use the guard access here?
			// XXX: do we need to synchronize this sending with the graph changes?
			incoming := obj.Graph.IncomingGraphVertices(obj.Func) // []Vertex
			if len(incoming) == 0 {
				// XXX: can changes in the graph shape occur such that we'd want to re-open this???
				once.Do(closeInput) // close early
				continue
			}
			ready := true // assume for now...
			si := &types.Type{
				// input to functions are structs
				Kind: types.KindStruct,
				Map:  obj.Func.Info().Sig.Map,
				Ord:  obj.Func.Info().Sig.Ord,
			}
			st := types.NewStruct(si)
			for _, v := range incoming {
				// XXX: what lock do we use the guard access here?
				args := obj.Graph.Adjacency()[v][obj.Func].(*interfaces.FuncEdge).Args
				obj.Mutex.RLock()
				f, ok := v.(interfaces.Func)
				if !ok {
					// programming error
					panic("vertex was not a Func")
				}
				value, exists := obj.Table[f]
				obj.Mutex.RUnlock()
				if !exists {
					ready = false // nope!
					break
				}

				// set each arg, since one value
				// could get used for multiple
				// function inputs (shared edge)
				for _, arg := range args {
					if err := st.Set(arg, value); err != nil { // populate struct
						panic(fmt.Sprintf("struct set failure on `%s` from `%s`: %v", obj, v, err))
					}
				}
			}
			if !ready {
				continue
			}

			select {
			case obj.Input<-st:
				// send new input
			case <-ctx.Done(): // if Stream dies, we would block here
				// pass, and let us exit elsewhere
			}

		case val, ok := <-obj.Output: // closed by Stream()
			if !ok {
				// wrap around and let streamChan close with the error
				//return nil
				obj.Output = nil
				continue
			}
			// XXX: I'm the only writer to this field, and there could be many readers...
			// XXX: we could use a different rwmutex for each vertex...
			// XXX: eg: obj.Mutex[obj].Lock() && obj.Mutex[obj].Unlock()
			obj.Mutex.Lock()
			obj.Table[obj.Func] = val
			obj.Mutex.Unlock()
			// now decide to send an event...
			if err := obj.init.Event(ctx); err != nil {
				continue // XXX: panic?
			}
			obj.init.Logf("sent event")

		// we wait for obj.Output to close for us to close...
		//case <-ctx.Done(): // when asked to exit
		//	return nil // we exit happily
		}
	}
}

// Event is called by the engine to tell this vertex that it is receiving an
// event. It should error if it can't receive the event for some reason. It must
// always be safe to call even after this vertex has closed.
func (obj *stateVertex) Event() error {
	select {
	case obj.eventsChan <- struct{}{}: // is this too simple a mechanism?
		return nil

	case <-obj.closedChan:
		return fmt.Errorf("already closed")
	}
}
