//  ---------------------------------------------------------------------------
//
//  main.go
//
//  Copyright (c) 2015, Jared Chavez.
//  All rights reserved.
//
//  Use of this source code is governed by a BSD-style
//  license that can be found in the LICENSE file.
//
//  -----------

// prun (parallel run) reads pipeline input, parses it into individual lines,
// and substitutes each line into a supplied command line, then
// executes the resulting commands in parallel across a configured
// number of concurrent goroutines.
// Usage: prun <worker count> <command>
//         <worker count> : The number of worker goroutines to run in parallel
//         <command> : The command line to run on each supplied input. the
//                     token '{}' is replaced by each line supplied on stdin.
//         example: find . -type f | prun stat {}
package main

import (
    "bytes"
    "fmt"
    "io"
    "math"
    "os"
    "os/exec"
    "strconv"
    "strings"

    "github.com/xaevman/app"
)

// RunJob structures contain all the command line data passed to prun,
// the argument for this run of the job, and the output and done channels
// on which to pass state back to the primary thread of the application.
type RunJob struct {
    Arg      string
    OutChan  chan string
    DoneChan chan int
    CmdData  []string
}

// Entry point
func main() {
    // validate args
    if len(os.Args) < 3 {
        printUsage()
        os.Exit(1)
    }

    // setup processing objects and counters
    workerCount, err := strconv.Atoi(os.Args[1])
    if err != nil {
        panic(err)
    }

    jobChan  := make(chan *RunJob, 0)
    doneChan := make(chan int, 0)
    outChan  := make(chan string, 0)
    doneCnt  := 0
    errCnt   := 0
    totalCnt := math.MaxInt32

    // start workers
    fmt.Printf("Starting %d workers...\n", workerCount)
    for i := 0; i < workerCount; i++ {
        go runWorker(i, jobChan)
    }

    // read pipeline input
    fmt.Println("Reading input...")
    go readInput(jobChan, outChan, doneChan, &totalCnt)

    // wait for results from all parsed commands
    func() {
        for doneCnt < totalCnt {
            select {
            case val := <-doneChan:
                errCnt += val
                doneCnt++

            case log := <-outChan:
                fmt.Print(log)
            }
        }
    }()

    // return code == the number of errors we encountered
    os.Exit(errCnt)
}

// argsToStr takes a list of string arguments and returns them concatenated
// into a single space delimited string.
func argsToStr(args ...string) string {
    var buffer bytes.Buffer
    for i := range args {
        buffer.WriteString(args[i])
        if i < len(args) - 1 {
            buffer.WriteString(" ")
        }
    }

    return buffer.String()
}

// printUsage prints help text for the application.
func printUsage() {
    fmt.Println("Usage:")
    fmt.Printf("\t%s <worker count> <command>\n", app.GetName())
    fmt.Println()
    fmt.Println("\tWhere <command> is the command to run for each input argument.")
    fmt.Printf("\tThe string '{}' within the command will be replaced with the argument")
    fmt.Println("comming in from the input pipeline.")
    fmt.Println()
    fmt.Printf("\texample: find . -type f | %s ls -alh {}\n", app.GetName())
}

// readInput runs within a goroutine and reads stdin, parsing
// it into discrete lines, and passing those lines as arguments
// to the worker goroutines for processing.
func readInput(
    jobChan  chan *RunJob, 
    outChan  chan string, 
    doneChan chan int, 
    totalCnt *int,
) {
    var buffer bytes.Buffer
    
    rb    := make([]byte, 1)
    count := 0
    
    // read until EOF
    for true {
        // read a byte
        _, err := os.Stdin.Read(rb)
        if err != nil {
            // if EOF, we're done
            if err == io.EOF {
                *totalCnt = count
                return
            }

            // something bad happened
            panic(err)
        }

        // end of a line? submit a RunJob to the workers
        if rb[0] == '\n' {
            count++

            rj         := new(RunJob)
            rj.Arg      = strings.Replace(strings.TrimSpace(buffer.String()), "\r" , "", -1)
            rj.OutChan  = outChan 
            rj.DoneChan = doneChan
            rj.CmdData  = make([]string, len(os.Args) - 2)
            copy(rj.CmdData, os.Args[2:])

            jobChan<- rj

            buffer.Reset()
            continue
        }

        // otherwise, keep writing to buffer
        _, err = buffer.Write(rb)
        if err != nil {
            panic(err)
        }
    }
}

// run takes a RunJob, parses out its command line parts, substitutes
// the pipeline argument into the command line, and then runs the command.
// run then outputs any stdout and stderr output and reports back to the main
// thread on completion.
func run(id int, job *RunJob) {
    if len(job.CmdData) < 1 {
        job.DoneChan<- 0
        return
    }

    if len(job.Arg) < 1 {
        job.DoneChan<- 0
        return
    }

    // replace the replacement token {}
    for i := range job.CmdData {
        if job.CmdData[i] == "{}" {
            job.CmdData[i] = job.Arg
        }
    }

    cmd      := exec.Command(job.CmdData[0], job.CmdData[1:]...)
    out, err := cmd.CombinedOutput()

    if err != nil {
        job.OutChan<- fmt.Sprintf("[%d] %s: %s\n", id, argsToStr(job.CmdData...), err)
        job.DoneChan<- 1
    } else {
        job.OutChan<- fmt.Sprintf("[%d] %s: %s\n", id, argsToStr(job.CmdData...), string(out))
        job.DoneChan<- 0
    }
}

// runWorker runs within a goroutine and simply waits for new RunJobs to be
// submitted to it.
func runWorker(id int, jobChan chan *RunJob) {
    for true {
        job := <-jobChan
        if len(job.Arg) < 1 {
            job.DoneChan<- 0
            continue
        }

        run(id, job)
    }
}
