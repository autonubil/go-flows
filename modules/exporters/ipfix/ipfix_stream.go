package ipfix

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/CN-TU/go-flows/flows"
	"github.com/CN-TU/go-flows/util"
	"github.com/CN-TU/go-ipfix"
)

type ipfixStreamExporter struct {
	id               string
	target           string
	protocol         string
	domain           uint32
	w                net.Conn
	interval         time.Duration
	templateLastSent time.Time

	writer    *ipfix.MessageStream
	allocated map[string]ipfix.InformationElement
	templates []int
	now       flows.DateTimeNanoseconds
}

func (pe *ipfixStreamExporter) Fields([]string) {}

// Export export given features
func (pe *ipfixStreamExporter) Export(template flows.Template, features []interface{}, when flows.DateTimeNanoseconds) {
	id := template.ID()
	if id >= len(pe.templates) {
		pe.templates = append(pe.templates, make([]int, id-len(pe.templates)+1)...)
	}
	templateID := pe.templates[id]
	if templateID == 0 {
		var err error
		template.InformationElements()
		templateID, err = pe.writer.AddTemplate(when, pe.AllocateIE(template.InformationElements())...)
		if err != nil {
			log.Panic(err)
		}
		pe.templates[id] = templateID
	} else {
		// resend template?
		if time.Since(pe.templateLastSent) > pe.interval {
			pe.templateLastSent = time.Now()
			pe.writer.SendTemplate(when, id)
			pe.writer.Flush(when)
		}
	}
	//TODO make templates for nil features
	pe.writer.SendData(when, templateID, features...)
	pe.now = when
}

// Finish Write outstanding data and wait for completion
func (pe *ipfixStreamExporter) Finish() {
	pe.writer.Flush(pe.now)
	pe.w.Close()
}

func (pe *ipfixStreamExporter) ID() string {
	return pe.id
}

func (pe *ipfixStreamExporter) AllocateIE(ies []ipfix.InformationElement) []ipfix.InformationElement {
	for i, ie := range ies {
		if ie.ID == 0 && ie.Pen == 0 { //Temporary Element
			if ie, ok := pe.allocated[ie.Name]; ok {
				ies[i] = ie
				continue
			}
			name := ie.Name
			ie = ipfix.InformationElement{
				Name:   normalizeName(name),
				Pen:    pen,
				ID:     uint16(len(pe.allocated)) + tmpBase,
				Type:   ie.Type,
				Length: ie.Length,
			}
			ies[i] = ie
			pe.allocated[name] = ie
		}
	}
	return ies
}

func (pe *ipfixStreamExporter) Init() {
	pe.allocated = make(map[string]ipfix.InformationElement)
	var err error
	pe.templateLastSent = time.Now().Add(-10 * time.Minute)

	conn, err := net.Dial(pe.protocol, pe.target)
	if err != nil {
		log.Fatal("Couldn't create ipfix message stream: ", err)
	}
	pe.w = conn

	pe.writer, err = ipfix.MakeMessageStream(pe.w, 1472, pe.domain)
	if err != nil {
		log.Fatal("Couldn't create ipfix message stream: ", err)
	}
	pe.templates = make([]int, 1)
}

func newipfixStreamExporter(args []string) (arguments []string, ret util.Module, err error) {
	set := flag.NewFlagSet("ipfix_steam", flag.ExitOnError)
	set.Usage = func() { ipfixStreamHelp("ipfix_stream") }
	protocol := set.String("protocol", "udp", "transport protocol")
	domainStr := set.String("domain", "1", "observation domain")
	intervalStr := set.String("template_interval", "300", "template resend interval")

	set.Parse(args)
	if set.NArg() < 1 {
		return nil, nil, errors.New("IPFIX exporter needs a target as argument")
	}
	target := set.Args()[0]

	domain, err := strconv.Atoi(*domainStr)
	if err != nil {
		return
	}
	interval, err := strconv.Atoi(*intervalStr)
	if err != nil {
		return
	}

	ipfix.LoadIANASpec()
	ret = &ipfixStreamExporter{id: "IPFIX|" + target, target: target, protocol: *protocol, interval: time.Duration(interval) * time.Second, domain: uint32(domain)}
	return
}

func ipfixStreamHelp(name string) {
	fmt.Fprintf(os.Stderr, `
The %s exporter writes the output to a ipfix file with a flow per line and a
header consisting of the feature description.

As argument, the output target is needed.

Usage:
  export [-protocol udp/tcp] [-domain 42] [-template_interval 300] %s target

Flags:
  -protocol string
    	Protocol to use to send data to a collector. Can be TCP or UDP
  -domain int
		Observation domain to report by
  -template_interval int
		Delay in seconds before the template is send 


`, name, name)
}

func init() {
	flows.RegisterExporter("ipfix_stream", "Exports flows to a ipfix datagram stream.", newipfixStreamExporter, ipfixhelp)
}
