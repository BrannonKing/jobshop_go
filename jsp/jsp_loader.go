package jsp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strconv"
)

type WorkPair struct {
	Machine int
	Delay   int
}

type Instance struct {
	Name     string       `json:"name"`
	Jobs     int          `json:"jobs"`
	Machines int          `json:"machines"`
	Optimum  int          `json:"optimum"`
	Path     string       `json:"path"`
	Work     [][]WorkPair `json:"work"`
}

func LoadInstances() []*Instance {
	var instances []*Instance
	fileBytes, _ := os.ReadFile("../../JSPLIB/instances.json")
	err := json.Unmarshal(fileBytes, &instances)
	if err != nil {
		panic(err)
	}

	for _, instance := range instances {
		file, err := os.Open("../../JSPLIB/" + instance.Path)
		if err != nil {
			panic(err)
		}
		reader := bufio.NewReader(file)
		for {
			line, _, err := reader.ReadLine()
			if err == io.EOF {
				break
			}
			if len(line) <= 0 || line[0] == '#' {
				continue
			}
			splits := bytes.Split(line, []byte(" "))
			if len(splits) <= 2 {
				continue
			}
			work := make([]WorkPair, 0, len(splits)/2)
			for j := 0; j < instance.Machines*2; j += 2 {
				machine, _ := strconv.Atoi(string(splits[j]))
				delay, _ := strconv.Atoi(string(splits[j+1]))
				work = append(work, WorkPair{Machine: machine, Delay: delay})
			}
			instance.Work = append(instance.Work, work)
		}
		_ = file.Close()
	}
	return instances
}
