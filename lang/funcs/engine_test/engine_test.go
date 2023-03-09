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

//go:build !root

package engine_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/purpleidea/mgmt/lang/funcs"
	_ "github.com/purpleidea/mgmt/lang/funcs/core" // register!
)

func TestFuncGraph1(t *testing.T) {
	t.Logf("Hello!")

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, err := funcs.Lookup("datetime.now")
	if err != nil {
		t.Errorf("could not find function: %+v", err)
		return
	}

	engine := &funcs.NewEngine{
		Name: "test",

		Debug: testing.Verbose(), // set via the -test.v flag to `go test`
		Logf: func(format string, v ...interface{}) {
			s := "test: " + fmt.Sprintf(format, v...)
			t.Logf("%s", s)
		},
	}

	t.Logf("Setup...")
	if err := engine.Setup(); err != nil {
		t.Errorf("could not setup engine: %+v", err)
		return
	}
	defer engine.Cleanup()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := engine.Run(ctx); err != nil {
			t.Errorf("error while running engine: %+v", err)
			return
		}
		t.Logf("engine shutdown cleanly...")
	}()

	// XXX: do we need an engine.Startup() signal?
	// XXX: this would prevent us calling Lock before Run started...
	// XXX: turns out it doesn't hurt to call Lock too early though...

	engine.Lock()

	engine.AddVertex(f) // the datetime.now function

	engine.Unlock()

	//	//t.Logf("Validate...")
	//	//if err := obj.Validate(); err != nil {
	//	//	t.Errorf("could not validate: %+v", err)
	//	//	return
	//	//}

	// wait for some activity
	t.Logf("Stream...")
	stream := engine.Stream()

	t.Logf("Loop...")
	count := 5 // let's wait for this many events...
	br := time.After(time.Duration(60) * time.Second)
Loop:
	for {
		select {
		case err, ok := <-stream:
			if !ok {
				t.Logf("Stream break...")
				break Loop
			}
			if err != nil {
				t.Logf("Error: %+v", err)
				continue
			}
			table := engine.Table()
			val, exists := table[f]
			if !exists {
				t.Errorf("stream value does not exist")
				continue
			}
			t.Logf("Stream value: %+v", val)
			if count == 0 {
				break Loop // success!
			}
			count--

		case <-br:
			t.Errorf("timeout while expecting streamed value")
			break Loop
		}
	}

	t.Logf("Closing...")
	cancel()

}
