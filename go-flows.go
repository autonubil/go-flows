package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strings"

	"strconv"

	"encoding/json"

	"pm.cn.tuwien.ac.at/ipfix/go-flows/exporters"
	"pm.cn.tuwien.ac.at/ipfix/go-flows/flows"
	"pm.cn.tuwien.ac.at/ipfix/go-flows/packet"
)

var (
	format        = flag.String("format", "text", "Output format (text|csv)")
	featurefile   = flag.String("features", "", "Features specification (json)")
	selection     = flag.String("select", "flows:0", "flow selection (key:number in specification)")
	output        = flag.String("output", "-", "Output filename")
	cpuprofile    = flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile    = flag.String("memprofile", "", "write mem profile to file")
	blockprofile  = flag.String("blockprofile", "", "write block profile to file")
	tracefile     = flag.String("trace", "", "set tracing file")
	numProcessing = flag.Uint("n", 4, "number of parallel processing queues")
	activeTimeout = flag.Uint("active", 1800, "active timeout in seconds")
	idleTimeout   = flag.Uint("idle", 300, "idle timeout in seconds")
	maxPacket     = flag.Uint("size", 9000, "Maximum packet size")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [args] file1 [file2] [...] \n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(-1)
}

func decodeFeatures(dec *json.Decoder) []interface{} {
	var ret []interface{}
	for {
		t, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if delim, ok := t.(json.Delim); ok {
			switch delim {
			case '{':
				ret = append(ret, decodeFeatures(dec))
			case '}':
				return ret
			}
		} else {
			ret = append(ret, t)
		}
	}
	panic("EOF")
}

func decodeJSON(inputfile, key string, id int) []interface{} {
	//var ret []interface{}
	f, err := os.Open(inputfile)
	if err != nil {
		log.Panic("Can't open ", inputfile)
	}
	defer f.Close()
	dec := json.NewDecoder(f)

	level := 0
	found := false
	discovered := 0

	for {
		t, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if delim, ok := t.(json.Delim); ok {
			switch delim {
			case '{', '[':
				level++
			case '}', ']':
				level--
			}
		}
		if field, ok := t.(string); ok {
			if found && level == 3 && field == "features" {
				if discovered == id {
					//dec.Decode(&ret)
					return decodeFeatures(dec)
				}
				discovered++
			} else if level == 1 && field == key {
				found = true
			}
		}
	}
	return nil
}

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		usage()
	}
	var exporter flows.Exporter
	switch *format {
	case "text":
		exporter = exporters.NewPrintExporter(*output)
	case "csv":
		exporter = exporters.NewCSVExporter(*output)
	default:
		usage()
	}
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	if *blockprofile != "" {
		f, err := os.Create(*blockprofile)
		if err != nil {
			log.Fatal(err)
		}
		runtime.SetBlockProfileRate(1)
		defer pprof.Lookup("block").WriteTo(f, 0)
	}
	if *tracefile != "" {
		f, err := os.Create(*tracefile)
		if err != nil {
			log.Fatal(err)
		}
		trace.Start(f)
		defer trace.Stop()
	}

	if *featurefile == "" {
		panic("Need a feature input file!")
	}

	selector := strings.Split(*selection, ":")
	if len(selector) != 2 {
		panic("select must be of form 'key:id'!")
	}

	selectorID, err := strconv.Atoi(selector[1])
	if err != nil {
		panic("select must be of form 'key:id'!")
	}

	features := decodeJSON(*featurefile, selector[0], selectorID)
	if features == nil {
		log.Panic("Features ", *selection, " not found in ", *featurefile)
	}

	flowtable := packet.NewParallelFlowTable(int(*numProcessing), flows.NewFeatureListCreator(features, exporter, flows.FeatureTypeFlow), packet.NewFlow, flows.Time(*activeTimeout)*flows.Seconds, flows.Time(*idleTimeout)*flows.Seconds, 100*flows.Seconds)

	time := packet.ReadFiles(flag.Args(), int(*maxPacket), flowtable)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
	flowtable.EOF(time)
	//_ = time
	exporter.Finish()
}
