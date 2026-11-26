// Command porcupine checks a client history of a simulated redwall run for
// strict serializability, with the Porcupine checker.
//
// Usage: porcupine [-html FILE] [-timeout DURATION] HISTORY.json
//
// The command prints "ok" and exits with status 0 if the history is
// strictly serializable. It prints "ILLEGAL" and exits with status 1 if the
// history is not strictly serializable. It prints "unknown" and exits with
// status 2 if the check did not finish within the time limit.
//
// The history format is described in dst/redwall_dst_history.erl.
//
// # Property
//
// A history is strictly serializable if the transactions have a total
// order with two properties. First, each transaction reads and writes as
// if the transactions ran one after another in that order. Second, a
// transaction that ended before another one started is before it in the
// order. This is linearizability of a database in which one operation is
// one transaction, so Porcupine can check it.
//
// # Model
//
// The state is the value of each key. A value is the number of the client
// whose transaction wrote the value, and 0 for a key without a value. Each
// client runs one transaction, so each value is written at most once.
//
// The command makes these operations from the row of a client:
//
//   - Outcome "committed": one operation "txn" from the start of the
//     transaction to the return of its commit. The operation is legal in a
//     state in which each key has the value that the client read. The
//     operation clears and sets the keys of the transaction.
//   - Outcome "read" or "not_committed": one operation "read" from the
//     start of the transaction to the end of its reads. The operation is
//     legal in a state in which each key has the value that the client
//     read. The operation changes nothing. redwall reads at one read
//     version, so the reads of a transaction that is not committed also
//     have one point in the order.
//   - Outcome "unknown": the operation "read" as above, and at most one
//     more operation, which starts at the end of the reads. See "Unknown
//     outcomes".
//
// # Unknown outcomes
//
// The second operation of a transaction with the outcome "unknown" depends
// on what the other clients read:
//
//   - A client read the value of the transaction. The transaction is then
//     committed, and the operation is "txn". The operation ends at the
//     earliest end of the reads of a client that read the value, because
//     the value was stored before that client read it.
//   - No client read the value, and the transaction cleared no key. The
//     command makes no second operation. This loses nothing. Suppose that
//     the history with the transaction has a legal order. No operation
//     between the transaction and the next write of a key of the
//     transaction reads that key, because it would read the value of the
//     transaction. The order without the transaction is therefore also
//     legal. A legal order without the transaction is a legal order of the
//     history, because the transaction may not be committed.
//   - No client read the value, and the transaction cleared a key. A read
//     of a key without a value can be a read of this clear. The operation
//     is "maybe", without an end: the next state is the same state, or the
//     state after the operation "txn" if that operation is legal.
//
// The model is nondeterministic because of "maybe". Porcupine converts it
// to a deterministic model whose state is a set of states.
//
// # Limitations
//
//   - A client that read the value of a transaction with the outcome
//     "unknown" makes that transaction committed in the model. The command
//     therefore accepts a read of a record that a recovery discarded
//     later, unless a later read shows that the value is gone.
//   - The history has the transactions of the 4 keys of the workload. The
//     transactions of the watches are not in the history.
//   - The check of a history with many "maybe" operations can take long.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
)

// The largest number of keys in a history.
const maxKeys = 8

type row struct {
	Client  int      `json:"client"`
	Start   int64    `json:"start"`
	ReadEnd int64    `json:"read_end"`
	End     int64    `json:"end"`
	Outcome string   `json:"outcome"`
	Reads   [][2]int `json:"reads"`
	Writes  []int    `json:"writes"`
	Clears  []int    `json:"clears"`
}

type state [maxKeys]int

type input struct {
	kind   string // "txn", "read" or "maybe"
	client int
	reads  [][2]int
	writes []int
	clears []int
}

func legal(s state, in input) bool {
	for _, r := range in.reads {
		if s[r[0]] != r[1] {
			return false
		}
	}
	return true
}

func apply(s state, in input) state {
	for _, k := range in.clears {
		s[k] = 0
	}
	for _, k := range in.writes {
		s[k] = in.client
	}
	return s
}

var model = porcupine.NondeterministicModel{
	Init: func() []interface{} { return []interface{}{state{}} },
	Step: func(st, i, _ interface{}) []interface{} {
		s := st.(state)
		in := i.(input)
		switch in.kind {
		case "read":
			if legal(s, in) {
				return []interface{}{s}
			}
			return nil
		case "txn":
			if legal(s, in) {
				return []interface{}{apply(s, in)}
			}
			return nil
		default:
			if legal(s, in) {
				return []interface{}{s, apply(s, in)}
			}
			return []interface{}{s}
		}
	},
	DescribeOperation: func(i, _ interface{}) string {
		in := i.(input)
		return fmt.Sprintf("%s of client %d: reads %v, clears %v, sets %v", in.kind, in.client, in.reads, in.clears, in.writes)
	},
	DescribeState: func(st interface{}) string { return fmt.Sprint(st.(state)) },
}

// A client read the value of a transaction before the commit of the
// transaction started.
var errIllegal = errors.New("a value was read before its transaction started to commit")

// operations converts the rows of a history to Porcupine operations. It
// returns errIllegal for a history that is not strictly serializable for
// the reason above, and another error for a row that the model cannot
// hold.
func operations(rows []row) ([]porcupine.Operation, error) {
	// For each value that a client read: the earliest end of the reads of
	// such a client.
	seen := map[int]int64{}
	var last int64
	for _, r := range rows {
		for _, read := range r.Reads {
			if read[0] < 0 || read[0] >= maxKeys {
				return nil, fmt.Errorf("client %d: key %d", r.Client, read[0])
			}
			if at, ok := seen[read[1]]; !ok || r.ReadEnd < at {
				seen[read[1]] = r.ReadEnd
			}
		}
		for _, k := range append(append([]int{}, r.Writes...), r.Clears...) {
			if k < 0 || k >= maxKeys {
				return nil, fmt.Errorf("client %d: key %d", r.Client, k)
			}
		}
		for _, t := range []int64{r.Start, r.ReadEnd, r.End} {
			if t > last {
				last = t
			}
		}
	}
	var ops []porcupine.Operation
	for _, r := range rows {
		if r.Client <= 0 || r.Start <= 0 || r.ReadEnd <= r.Start {
			return nil, fmt.Errorf("client %d: start %d, read_end %d", r.Client, r.Start, r.ReadEnd)
		}
		in := input{client: r.Client, reads: r.Reads, writes: r.Writes, clears: r.Clears}
		switch r.Outcome {
		case "committed":
			if r.End <= r.ReadEnd {
				return nil, fmt.Errorf("client %d: end %d", r.Client, r.End)
			}
			in.kind = "txn"
			ops = append(ops, porcupine.Operation{ClientId: r.Client, Input: in, Call: r.Start, Return: r.End})
		case "read", "not_committed", "unknown":
			in.kind = "read"
			ops = append(ops, porcupine.Operation{ClientId: r.Client, Input: in, Call: r.Start, Return: r.ReadEnd})
			if r.Outcome != "unknown" {
				continue
			}
			second := in
			if at, ok := seen[r.Client]; ok {
				if at <= r.ReadEnd {
					return nil, errIllegal
				}
				second.kind = "txn"
				ops = append(ops, porcupine.Operation{ClientId: r.Client, Input: second, Call: r.ReadEnd, Return: at})
			} else if len(r.Clears) > 0 {
				// The operation has no end: it returns after every
				// other event of the history.
				second.kind = "maybe"
				last++
				ops = append(ops, porcupine.Operation{ClientId: r.Client, Input: second, Call: r.ReadEnd, Return: last})
			}
		default:
			return nil, fmt.Errorf("client %d: outcome %q", r.Client, r.Outcome)
		}
	}
	return ops, nil
}

func main() {
	html := flag.String("html", "", "write a visualization of the history to this file")
	limit := flag.Duration("timeout", 60*time.Second, "time limit for the check")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: porcupine [-html FILE] [-timeout DURATION] HISTORY.json")
		os.Exit(3)
	}
	data, err := os.ReadFile(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	var rows []row
	if err := json.Unmarshal(data, &rows); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	ops, err := operations(rows)
	if err == errIllegal {
		fmt.Printf("ILLEGAL %d operations\n", len(rows))
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	m := model.ToModel()
	result, info := porcupine.CheckOperationsVerbose(m, ops, *limit)
	if *html != "" {
		if err := porcupine.VisualizePath(m, info, *html); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
	switch result {
	case porcupine.Ok:
		fmt.Printf("ok %d operations\n", len(ops))
	case porcupine.Illegal:
		fmt.Printf("ILLEGAL %d operations\n", len(ops))
		os.Exit(1)
	default:
		fmt.Printf("unknown %d operations\n", len(ops))
		os.Exit(2)
	}
}
