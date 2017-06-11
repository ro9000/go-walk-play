package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {

	processMapFile()

}

// comma inserts commas in a non-negative decimal integer string.
func commaRecursive(s string) string {
	fmt.Println("commaRecursive", s)
	n := len(s)
	if n <= 3 {
		return s
	}
	return commaRecursive(s[:n-3]) + "," + s[n-3:]
}

func commaIterative(s string) string {
	fmt.Println("commaIterative", s)

	counter := 1

	n := len(s) - 1
	for ; n >= 0; n-- {
		//fmt.Println(n, s[n])
		fmt.Printf("%d\t%q\t%d\n", n, s[n], s[n])
		if counter == 3 {
			counter = 1
			fmt.Println("COMMA")
		} else {
			counter++
		}
	}

	s = "234000999"
	fmt.Println("len is", len(s))
	w := len(s) / 3
	fmt.Println("w is", w)
	q := len(s) % 3
	fmt.Println("q is", q)

	return "sssssss"
}

func processMapFile() {

	log.Println("PROCESSMAPFILE")

	mapfn := "/home/ro/into-the-deep-test-files/box-maps/2009-10-30--01-23-38--E__data.map"
	log.Println("map file to process:", mapfn)

	finfo, err := os.Stat(mapfn)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("good stat on file, stat:", finfo)
	log.Println("basename:", finfo.Name(), "size:", finfo.Size(), "bytes")

	fin, err := os.Open(mapfn)
	if err != nil {
		log.Fatal(err)
	}
	defer fin.Close()

	input := bufio.NewReader(fin)

	fmt.Printf("%T\n", input)

	//parseMapLines(input)
	//parseMapLinesUsingStrings(input)

	// good
	//justParseLines(input)

	fmt.Println("========================COOL========================")

	xBuildMap(input)

}

func parseMapLines(input *bufio.Reader) {

	n := 0
	var line []byte
	var err error

	sep := []byte("|")
	newsep := []byte("||")

	for {
		line, err = input.ReadBytes('\n')
		n++
		elz := bytes.Split(line, sep)
		fmt.Println(len(elz))
		fmt.Println(string(bytes.Join(elz, newsep)))

		//fmt.Println(string(line))

		if err == io.EOF {
			break
		}
	}

	fmt.Println(n, line)

}

func parseMapLinesUsingStrings(input *bufio.Reader) {

	n := 0
	line := ""
	var err error

	sep := "|"
	newsep := "||"

	for {
		line, err = input.ReadString('\n')
		n++
		elz := strings.Split(line, sep)
		fmt.Println(len(elz))
		fmt.Println(strings.Join(elz, newsep))

		if err == io.EOF {
			break
		}
	}

	fmt.Println(n, line)

}

type string2string map[string][]string

func justParseLines(input *bufio.Reader) {

	dir2dirs := make(string2string)
	dir2files := make(string2string)
	//	fmt.Println(dir2dirs)
	//	fmt.Println(dir2files)
	//	os.Exit(0)

	n := 0

	for {
		line, err := input.ReadString('\n')
		n++

		line = strings.Trim(line, " \r\n") // trim space cr lf

		if line != "" {
			justParseSingleLine(line, n, dir2dirs, dir2files)
		}

		if err == io.EOF {
			break
		}
	}

	log.Printf("%d lines processed\n", n)

	printJustMap(dir2dirs)
	printJustMap(dir2files)

	time.Sleep(time.Millisecond * 60000)
	//_ = time.Sleep

	return
}

func printJustMap(dirmap string2string) {

	var odirs []string

	for key := range dirmap {
		odirs = append(odirs, key)
	}
	sort.Strings(odirs)

	for _, key := range odirs {
		val := dirmap[key]
		fmt.Printf("\n\n")
		fmt.Println(key)
		for _, x := range val {
			fmt.Printf("  %q", x)
		}

	}

}

func justParseSingleLine(line string, linenum int, dir2dirs, dir2files string2string) {

	elz := strings.Split(line, "|")

	if len(elz) != 6 {
		log.Printf("fuckety not six elements in map file, there were %d elements", len(elz))
		log.Fatalf("line number %d looks like this: >%s<", linenum, line)
	}

	xpath := elz[0]
	xtype := elz[1] // d or f, dir or file
	xsize := elz[2]
	//xmd5   := elz[3]
	xmtime := elz[4]
	//xattr  := elz[5]

	pathList := strings.Split(xpath, "\\")
	/*
		for i, x := range pathList {
			fmt.Println(i, x)
		}
	*/

	lastPath := pathList[len(pathList)-1]
	fmt.Println("    lastPath:", lastPath)

	parentPath := strings.Join(pathList[:len(pathList)-1], "\\")
	fmt.Println("    parentPath:", parentPath)

	if xtype == "d" {
		fmt.Println("Dir:", xpath)
		//dir2dirs[parentPath] = dir2dirs[parentPath] + " LOL " + lastPath // concat lol
		dir2dirs[parentPath] = append(dir2dirs[parentPath], lastPath)
	} else {
		isize, err := strconv.ParseInt(xsize, 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  File: %s %d %s\n", xpath, isize, xmtime)
		//dir2files[parentPath] = dir2files[parentPath] + " X " + lastPath // more concat lolz
		dir2files[parentPath] = append(dir2files[parentPath], lastPath)
	}

	//fmt.Println(len(strings.Split(xpath, "\\")), "path components")

}

func (ddx ddxMaster) queueLen() int {
	acc := 0
	for i := 0; i < len(ddx); i++ {
		acc += len(ddx[i])
	}
	return acc
}

func (pddx *ddxMaster) enqueue(sub ddxSub) {
	*pddx = append(*pddx, sub)
}

func (pddx *ddxMaster) next() *xDir {

	ddx := *pddx

	if len(ddx) == 0 {
		return nil
	}

	// first item of first sublist
	sub := ddx[0]
	next := sub[0]

	//fmt.Println("  queueLen() len:", pddx.queueLen())
	//fmt.Println("  next() sublist len:", len(sub))

	if len(sub) == 1 {
		//fmt.Println("  EQUALS 1!")
		ddx = ddx[1:]
	} else {
		ddx[0] = sub[1:]
	}

	*pddx = ddx

	return next
}

type ddxMaster []ddxSub
type ddxSub []*xDir

type qring struct {
	buf     []*xDir
	numused int
	first   int
	last    int
}

func newQring() *qring {
	q := new(qring)
	q.buf = make([]*xDir, 20, 20)
	q.numused = 0
	q.first = 0
	q.last = 0
	return q
}

func (q *qring) len() int {
	return len(q.buf)
}

func (q *qring) cap() int {
	return cap(q.buf)
}

func encopy(dest []*xDir, srcs ...[]*xDir) qring {

	ix := 0

	for _, src := range srcs {
		copy(dest[ix:], src)
		ix += len(src)
	}

	return qring{buf: dest, numused: ix, first: 0, last: ix - 1}
}

func (q *qring) enqueue(zxd []*xDir) {

	fmt.Printf("\nenqueue %d items: %v\n", len(zxd), zxd)

	q.printStats()

	// no items to add, just return
	if len(zxd) == 0 {
		fmt.Println("nothing to add, returning")
		return
	}

	// if queue is empty
	if q.numused == 0 {

		fmt.Println("queue is empty")

		// if buffer not big enough, expand it
		if len(zxd) > len(q.buf) {
			newsize := len(zxd) + 10
			q.buf = make([]*xDir, newsize)
			fmt.Printf("expanded to %d\n", newsize)
		}

		// copy items into buffer now
		numcopied := copy(q.buf, zxd)
		q.numused = len(zxd)
		q.first = 0
		q.last = len(zxd) - 1
		fmt.Println("numcopied:", numcopied)
		q.printStats()
		return
	}

	// queue is not empty
	fmt.Println("queue is not empty")

	// if buffer not big enough to add these new items
	if q.numused+len(zxd) > len(q.buf) {

		fmt.Println("buffer needs to be expanded and reshuffled")

		newsize := q.numused + len(zxd) + 10
		newbuf := make([]*xDir, newsize)
		fmt.Printf("expanded to %d and ready to reshuffle\n", newsize)

		if q.first <= q.last {
			*q = encopy(newbuf, q.buf[q.first:q.last+1], zxd)
		} else {
			*q = encopy(newbuf, q.buf[q.first:], q.buf[:q.last+1], zxd)
		}

		// buffer is big enough to add these new items
	} else {

		fmt.Println("buffer is big enough to add these new items")

		if q.first <= q.last {

			fmt.Println("q.first <= q.last")

			// two segments where items can go
			// space from last to end
			// space from 0 to first

			sliceA := q.buf[q.last+1:]
			sliceB := q.buf[:q.first]

			fmt.Println("two segments where items can go")
			fmt.Println("sliceA:", sliceA, "len:", len(sliceA))
			fmt.Println("sliceB:", sliceB, "len:", len(sliceB))

			// if new items will have to wrap over
			if len(zxd) > len(sliceA) {

				zxdA := zxd[:len(sliceA)]
				zxdB := zxd[len(sliceA):]

				fmt.Println("zxdA:", zxdA, "len:", len(zxdA))
				fmt.Println("zxdB:", zxdB, "len:", len(zxdB))

				copy(sliceA, zxdA)
				copy(sliceB, zxdB)
				q.last = len(zxdB) - 1
				q.numused += len(zxdA) + len(zxdB)

				// if only need to copy to the end, no wrap
			} else {

				copy(sliceA, zxd)
				q.last += len(zxd)
				q.numused += len(zxd)

			}

		} else {

			fmt.Println("else, q.first > q.last")

			// one segment where items can go
			// space from last to first

			sliceK := q.buf[q.last+1 : q.first]

			fmt.Println("only one segment where items can go")
			fmt.Println("sliceK:", sliceK, "len:", len(sliceK))

			copy(sliceK, zxd)
			q.last += len(zxd)
			q.numused += len(zxd)

		}

	}

	q.printStats()
	return
}

func printOrderedAux(zxd []*xDir) {
	for _, xd := range zxd {
		fmt.Printf("%p ", xd)
	}

}
func (q *qring) printOrdered() {

	if q.numused == 0 {
		fmt.Println("empty")
		return
	}

	if q.first <= q.last {
		printOrderedAux(q.buf[q.first : q.last+1])
	} else {
		printOrderedAux(q.buf[q.first:])
		printOrderedAux(q.buf[:q.last+1])
	}
	fmt.Println("")
}

func (q *qring) printStats() {
	//return
	fmt.Println("printStats:")
	fmt.Printf("  ordered: ")
	q.printOrdered()
	fmt.Printf("  %v\n", q.buf)
	fmt.Printf("  numused: %v, first: %v, last: %v\n", q.numused, q.first, q.last)
	fmt.Printf("  len: %v, cap: %v\n", q.len(), q.cap())
	fmt.Printf("  %p %p\n", q.buf, &q.buf[0])
}

func (q *qring) dequeue() *xDir {

	fmt.Println("\ndequeue")
	q.printStats()

	if q.numused == 0 {
		return nil
	}

	q.numused--

	item := q.buf[q.first]
	q.buf[q.first] = nil

	if q.numused == 0 {
		q.first = 0
		q.last = 0
	} else {
		q.first++
		if q.first == len(q.buf) {
			q.first = 0
		}
	}

	fmt.Printf("item: %p\n", item)
	q.printStats()

	return item
}

type xDir struct {
	parentDir *xDir
	rr        int
	mtime     int64
	childDirs []*xDir
	files     []*xFile
}

type xFile struct {
	parentDir *xDir
	basename  string
	mtime     int64
	size      int64
}

var next_rr int
var ss2rr map[string]int
var rr2ss map[int]string

func get_ss(rr int) string {
	return rr2ss[rr]
}

func get_rr(ss string) int {

	rr, ok := ss2rr[ss]
	if !ok {
		rr = next_rr
		rr2ss[rr] = ss
		ss2rr[ss] = rr
		next_rr++
	} else {
		//fmt.Println("have this already:", ss)
	}

	return rr
}

var path2dir map[int]*xDir
var rootDir xDir

var allDirs []*xDir
var allFiles []*xFile

func xBuildMap(input *bufio.Reader) {

	ss2rr = make(map[string]int)
	rr2ss = make(map[int]string)
	_ = get_rr("") // initialize root dir, toplevel

	path2dir = make(map[int]*xDir)

	rootDir = xDir{rr: 0}

	path2dir[0] = &rootDir

	allDirs = append(allDirs, &rootDir)

	n := 0

	for {
		line, err := input.ReadString('\n')
		n++

		line = strings.Trim(line, " \r\n") // trim space cr lf

		if line != "" {
			xBuildMapParseSingleLine(line, n)
		}

		if err == io.EOF {
			break
		}
	}

	log.Printf("%d lines processed\n", n)

	xBuildMapWalkIt()

	log.Println("done process, now waiting")

	time.Sleep(time.Millisecond * 60000)

}

func xBuildMapParseSingleLine(line string, linenum int) {

	// fmt.Printf("%d: %s\n", linenum, line)

	elz := strings.Split(line, "|")

	if len(elz) != 6 {
		log.Printf("fuckety not six elements in map file, there were %d elements", len(elz))
		log.Fatalf("line number %d looks like this: >%s<", linenum, line)
	}

	xpath := elz[0]
	xtype := elz[1] // d or f, dir or file
	xsize := elz[2]
	//xmd5   := elz[3]
	xmtime := elz[4]
	//xattr  := elz[5]

	imtime, err := strconv.ParseInt(xmtime, 10, 64)
	if err != nil {
		log.Fatal(err)
	}

	pathList := strings.Split(xpath, "\\")
	lastPath := pathList[len(pathList)-1]
	parentPath := strings.Join(pathList[:len(pathList)-1], "\\")

	parent_rr := get_rr(parentPath)

	pdir, ok := path2dir[parent_rr]
	if !ok {
		log.Fatalf("not seen parent path yet: %q", parentPath)
	}

	if xtype == "d" {

		this_rr := get_rr(xpath)

		xdir := xDir{parentDir: pdir, rr: this_rr, mtime: imtime}
		path2dir[this_rr] = &xdir
		pdir.childDirs = append(pdir.childDirs, &xdir)

		allDirs = append(allDirs, &xdir)

	} else {

		isize, err := strconv.ParseInt(xsize, 10, 64)
		if err != nil {
			log.Fatal(err)
		}

		xfile := xFile{parentDir: pdir, basename: lastPath, mtime: imtime, size: isize}
		pdir.files = append(pdir.files, &xfile)

		allFiles = append(allFiles, &xfile)

	}

}

func xBuildMapWalkIt() {

	fmt.Println("count  dirs:", len(allDirs))
	fmt.Println("count files:", len(allFiles))

	fmt.Println("next_rr is", next_rr)

	// use a queue of lists of dirs, reusing childDirs

	if false {

		//ddx := [][]*xDir{}
		ddx := ddxMaster{}

		level1dirs := path2dir[0].childDirs
		ddx = append(ddx, level1dirs)

		level1dirs = level1dirs[1:]
		fmt.Println(path2dir[0].childDirs)
		fmt.Println(level1dirs)

	}

	if false {

		ddx := ddxMaster{}

		//	ddx = append(ddx, path2dir[0].childDirs)
		//	ddx = append(ddx, path2dir[1].childDirs)
		//	ddx = append(ddx, path2dir[2].childDirs)
		ddx.enqueue(path2dir[0].childDirs)
		ddx.enqueue(path2dir[1].childDirs)
		ddx.enqueue(path2dir[2].childDirs)

		fmt.Println(ddx)
		fmt.Println(ddx.queueLen())

		next := ddx.next()
		for ; next != nil; next = ddx.next() {
			fmt.Printf("  %p\n", next)
			fmt.Println("len:", ddx.queueLen())
		}

		fmt.Println(ddx)
		fmt.Println(ddx.queueLen())

	}

	// new queue implementation, ring buffer

	qring := newQring()

	qring.enqueue(path2dir[0].childDirs)

	if false {
		qring.enqueue(path2dir[0].childDirs[0:21])
		qring.dequeue()
		qring.dequeue()
		qring.dequeue()
		qring.dequeue()
		qring.enqueue(path2dir[0].childDirs[0:11])
		qring.enqueue(path2dir[0].childDirs[0:2])
		qring.enqueue(path2dir[0].childDirs[0:1])
		qring.enqueue(path2dir[0].childDirs[0:1])
	}

	//	qring.enqueue(path2dir[0].childDirs)
	//	qring.enqueue(path2dir[0].childDirs)
	//	qring.enqueue(path2dir[0].childDirs)

	if false {
		qring.enqueue(path2dir[0].childDirs)
		qring.enqueue(path2dir[0].childDirs)
		return
	}

	if false {
		for _, xd := range allDirs {
			qring.enqueue(xd.childDirs)
		}
		return
	}

	setX := map[string]bool{}

	ndircount := 0

	for next := qring.dequeue(); next != nil; next = qring.dequeue() {

		ndircount++

		dirname := get_ss(next.rr)
		setX[dirname] = true
		fmt.Println(dirname)

		if len(next.childDirs) > 0 {
			qring.enqueue(next.childDirs)
		}

	}

	fmt.Println("ndircount:", ndircount)

	return

	// use iteration, and a queue, to walk it

	setQ := map[string]bool{}

	queue := ddxMaster{}
	queue = append(queue, path2dir[0].childDirs)

	qmax := len(queue)
	qqmax := queue.queueLen()

	next := queue.next()
	for ; next != nil; next = queue.next() {
		//fmt.Println(queue)
		//fmt.Println("  ", next)

		//fmt.Printf("%v\n", queue)
		//fmt.Printf("  %v\n", next)
		//fmt.Printf("  =>  %v\n", next.childDirs)

		if len(queue) > qmax {
			qmax = len(queue)
		}
		if queue.queueLen() > qqmax {
			qqmax = queue.queueLen()
		}

		dirname := get_ss(next.rr)
		fmt.Println(dirname)
		setQ[dirname] = true

		if len(next.childDirs) > 0 {
			queue = append(queue, next.childDirs)
		}

	}

	fmt.Println("qmax:", qmax)
	fmt.Println("qqmax:", qqmax)

	// use iteration, and a stack, to walk it

	setS := map[string]bool{}

	stack := []*xDir{}
	stack = append(stack, path2dir[0]) // initialize with root

	smax := len(stack)
	for len(stack) > 0 {

		if len(stack) > smax {
			smax = len(stack)
		}

		fmt.Printf("stack len: %d, cap: %d\n", len(stack), cap(stack))
		fmt.Printf("stack before: %v\n", stack)
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		fmt.Printf("stack  after: %v\n", stack)

		fmt.Printf("  VISIT: %p %v\n", top, top)

		dirname := get_ss(top.rr)
		fmt.Println(dirname)
		setS[dirname] = true

		for i := 0; i < len(top.childDirs); i++ {
			stack = append(stack, top.childDirs[i])
		}

		fmt.Printf("stack    now: %v\n", stack)

	}

	fmt.Println("stack max is", smax)

	// compare sets

	fmt.Println("len(setX):", len(setX))
	fmt.Println("len(setQ):", len(setQ))
	fmt.Println("len(setS):", len(setS))

	for k, _ := range setQ {
		if _, ok := setS[k]; !ok {
			fmt.Printf(">%s< not in setS\n", k)
		}
	}
	for k, _ := range setS {
		if _, ok := setQ[k]; !ok {
			fmt.Printf(">%s< not in setQ\n", k)
		}
	}

	fmt.Println("=--=")

	for k, _ := range setX {
		if _, ok := setS[k]; !ok {
			fmt.Printf(">%s< not in setS\n", k)
		}
	}
	for k, _ := range setS {
		if _, ok := setX[k]; !ok {
			fmt.Printf(">%s< not in setX\n", k)
		}
	}

	if false {

		// use recursive closure to walk it

		acc := []int{}

		var walkDir func(*xDir)
		walkDir = func(xd *xDir) {
			//fmt.Printf("%v\n", xd)
			acc = append(acc, xd.rr)
			for i := 0; i < len(xd.childDirs); i++ {
				walkDir(xd.childDirs[i])
				//acc = append(acc, xd.childDirs[i].rr)
			}
			//acc = append(acc, xd.rr)
		}
		// equivalent lines:
		walkDir(path2dir[0])
		//walkDir(&rootDir)

		for _, rr := range acc {
			fmt.Printf("%d => %s\n", rr, rr2ss[rr])
		}

	}

	if false {

		for rr, ss := range rr2ss {
			fmt.Printf("%d => %s\n", rr, ss)
		}

		os.Exit(0)
	}

	// NEXT ITEM: Walk from root down, depth-first and breadth-first, both

	if false {
		for _, el := range allFiles {
			fmt.Printf("%v\n", el)
		}
	}

	if false {
		for _, v := range allDirs {

			fmt.Printf("%v\n", v)
			for _, el := range v.childDirs {
				fmt.Printf("     DIR: %v\n", el)
			}
			for _, el := range v.files {
				fmt.Printf("    FILE: %v\n", el)
			}
		}
	}

	// extract intpaths from path2dir

	var intpaths []int
	for i := range path2dir {
		intpaths = append(intpaths, i)
	}
	sort.Ints(intpaths)

	if false {
		for i, v := range intpaths {
			fmt.Println(i, "=>", v)
		}
	}

	if false {
		// unsorted
		for k, v := range path2dir {
			fmt.Printf("%d => %v\n", k, v)
			for _, el := range v.childDirs {
				fmt.Printf("     DIR: %v\n", el)
			}
			for _, el := range v.files {
				fmt.Printf("    FILE: %v\n", el)
			}
		}
	}

}
