package log

import (
	"fmt"
	api "github.com/vsivarajah/distributed_services/api/v1"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

//The log consists of a list of segments and a pointer to the active segment to append writes to.
//The directory is where we store the segments
type Log struct {
	mu sync.RWMutex
	Dir string
	Config Config

	activeSegment *segment
	segments []*segment
}

//NewLog sets up defaults for the config file that the caller didn't specify
//Create a log instance and set up that instance
func NewLog(dir string, c Config) (*Log, error)  {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir: dir,
		Config: c,
	}
	return l, l.setup()
}


//When a log starts, it's responsible for setting itself up for the segments that already exist on disk
//or if the log is new and has no existing segments, for bootstrapping the initial segment.
//we fetch the list of the segments on disk, parse and sort the base offsets (because we want our slice of segments
//to be in order from oldest to newest)
//and then create the segments with newSegment helper method, which creates a segment for the base offset you pass in.
func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
			)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i,j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		//baseOffset contains dup for index and store so we skip the dup
		i++
	}
	if l.segments == nil {
		if err = l.newSegment(
			l.Config.Segment.InitialOffset,
			); err != nil {
			return err
		}
	}
	return nil
}

//Append appends a record to the log. We append the record to the active segment.
//If the segment is at its max size, then we make a new active segment.
//We are wrapping Append and subsequent funcs with a mutex to coordinate access to this section of the code.
//We use a RWMutex to grant access to reads when there isn't a write holding the lock.
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed(){
		err = l.newSegment(off +1)
	}
	return off, err
}

//Read reads the record stored at the given offset. In Read, we first find
//the segmment that contains the given record. Sinze the segments are in order from oldest to newest
//and the segment's base offset is the smalest offset in the segment, we iterate
//over the segments until we find the first segment whose base offset is less than or equal to the offset we're looking for.
//Once we know the segment that contains the record, we get the index entry from the segment's index,
//and we read the data out of the segment's store file.
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return s.Read(off)

}

//Close iterates over the segments and closes them
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

//Remove closes the log and then removes its data
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

//Reset removes the log and then creates a new log to replace it
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

//these methods tell us the offset range stored in the log.
//Used when finding the nodes with the oldest and newest data and which nodes are falling behind.
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

//these methods tell us the offset range stored in the log.
//Used when finding the nodes with the oldest and newest data and which nodes are falling behind.
func (l *Log) HighestOffset() (uint64, error){
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off -1, nil
}

//Truncate removes all segments whose highest offset is lower than lowest.
//Because we don't have disks with infinite space.
//We'll periodically call Truncate() to remove old segments whose data we (hopefully) have processed
//by then and dont need anymore
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

//Reader returns an io.Reader to read the whole log. We'll need this capability when we implement coordinate consensus.
//and need to support snapshots and restoring a log.
//Reader uses an io.MultiReader() call to concatenate the segment's stores.
//The segment stores are wrapped by the originReader type for two reasons.
//The first reason is to satisfy the io.Reader interface so we can pass it into the io.MultiReader call.
//the second reason is to ensure that we begin reading from the origin of the store and read its entire file.
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

//newSegment creates a new segment, appends that segment to the log's slice of segments
//and makes the new segment the active segment so that subsequent append calls write to it.
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}