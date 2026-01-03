package main

import(
	"fmt"
	"encoding/binary"
	"io"
	"log"
	"os"
	"math/rand"
	"time"
)


const (
	OpPut byte = 0
	OpDelete byte = 1
	blockSize= 4096
	maxLevel= 16
	probability= 0.5
)
type SSTableBuilder struct{
	file *os.File
	currentBlock []byte
	blockIndex []IndexEntry
	currentOffset int64
}

type IndexEntry struct{
	key string
	offset int64
}

func newSSTableBuilder(filename string) (*SSTableBuilder, error){
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return &SSTableBuilder{
		file: f,
		currentBlock: make([]byte,0, blockSize),
		blockIndex: make([]IndexEntry, 0),
		currentOffset: 0,
	}, nil
}

func (b *SSTableBuilder) Add(key string, value string) error{
	record := serializeRecord(key, value, OpPut)
	// op := record[0]
	record = record[1:]
	if (len(b.currentBlock)+len(record)) > blockSize {
		err := b.flushBlock()
		if err != nil {
			return err
		}
	}
	if len(b.blockIndex)==0{
		b.blockIndex = append(b.blockIndex, IndexEntry{
			key: key, 
			offset: b.currentOffset,
		})
	}
	b.currentBlock = append(b.currentBlock, record...)
	return nil
}


func (b *SSTableBuilder) flushBlock() error{
	if len(b.currentBlock)==0{
		return nil
	}
	n, err := b.file.Write(b.currentBlock)
	b.currentBlock = b.currentBlock[:0]
	b.currentOffset += int64(n)
	if err != nil {
		return err
	}
}

func (b *SSTableBuilder) finish() error {
	// 1. Flush any remaining data in the buffer to the file
	if err := b.flushBlock(); err != nil {
		return err
	}

	indexStartOffset := b.currentOffset


	for _, entry := range b.blockIndex {
		
		keyBytes := []byte(entry.key)
		offset := entry.offset
		entrySize := 4 + len(keyBytes) + 8
		buf := make([]byte, entrySize)


		binary.BigEndian.PutUint32(buf[0:4], uint32(len(keyBytes)))
		
		copy(buf[4:], keyBytes)
		
		// Write Offset (Where does the data block start?)
		binary.BigEndian.PutUint64(buf[4+len(keyBytes):], uint64(offset))


		n, err := b.file.Write(buf)
		if err != nil { return err }
		
		b.currentOffset += int64(n)
	}

	footer := make([]byte, 8)
	binary.BigEndian.PutUint64(footer, uint64(indexStartOffset))
	
	_, err := b.file.Write(footer)
	if err != nil { return err }

	return b.file.Close()
}



func writeToWAL(key string, value string, op byte) (bool, error){
	filename := "wal.log"
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return false, err
	}
	defer file.Close()
	record := serializeRecord(key, value, OpPut)
	_, err = file.Write(record)
	if err != nil {
		return false, err
	}
	//sync the file
	err = file.Sync()
	if err != nil {
		return false, err
	}
	return true, nil
}

// ReplayWAL reads the WAL file and populates the MemTable
func (m *MemTable) ReplayWAL(filename string) error {
	file, err := os.Open(filename) // Use Open for read-only
	if os.IsNotExist(err) {
		return nil // No WAL exists yet, that's fine (fresh start)
	}
	if err != nil {
		return err
	}
	defer file.Close()

	var offset int64 = 0
	
	for {
		// 1. Read OpType (1 byte)
		header := make([]byte, 1)
		_, err := file.ReadAt(header, offset)
		if err == io.EOF {
			break // End of file, we are done
		}
		if err != nil {
			return err
		}
		op := header[0]
		offset += 1

		// 2. Read Key Size and Value Size (8 bytes)
		sizes := make([]byte, 8)
		_, err = file.ReadAt(sizes, offset)
		if err != nil { return err }

		
		keySize := binary.BigEndian.Uint32(sizes[0:4])
		valueSize := binary.BigEndian.Uint32(sizes[4:8])
		offset += 8

		// 3. Read Key and Value
		data := make([]byte, keySize+valueSize)
		_, err = file.ReadAt(data, offset)
		if err != nil { return err }
		
		key := string(data[:keySize])
		value := string(data[keySize:]) // Note: value is now string, assuming you converted Node.value to string
        
		offset += int64(keySize + valueSize)

	
		if op == OpPut {
			m.data.Insert(key, value) 
		} else if op == OpDelete {
			m.data.Delete(key)
		}
	}
	return nil
}

type MemTable struct {
	data *SkipList
	size int64
	mu sync.Mutex
}

func newMemTable() *MemTable {
	return &MemTable{
		data: newSkipList(),
		size: 0,
	}
}

func (m *MemTable) Set(key string, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	success, err := writeToWAL(key, value, OpPut);
	if !success {
		log.Fatal(err)
		return err
	}
	m.data.Insert(key, value)
	m.size+= int64(len(key)+len(value))
	if m.size > 10 * 1024 * 1024 {
		println("Memtable size exceeded 10MB! Flushing to SSTable")
		err := m.flushToSSTable()
		if err != nil { return err }
		m.size = 0
	}
	return nil
}

func (m *MemTable) flushToSSTable() error{
	builder, err := newSSTableBuilder("data_1.sst")
	iter:= m.data.Iterator()
	if err != nil { return err }
	for node := range iter {
		builder.Add(node.key, node.value)
	}
	err = builder.finish()
	if err != nil { return err }
	m.data = newSkipList()
	m.size = 0
	os.Remove("wal.log")
	return nil
}

func (m *MemTable) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data.Search(key)
}

func (m *MemTable) Delete(key string)  error {
	m.mu.Lock()
	defer m.mu.Unlock()
	success, err := writeToWAL(key, "", OpDelete);
	if !success {
		log.Fatal(err)
		return err
	}
	
	err = m.data.Delete(key)
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

type Node struct {
	key string
	value string
	forward []*Node
}
type SkipList struct {
	head *Node
	level int
	rnd *rand.Rand
}
func newSkipList() *SkipList {
	return &SkipList{
		head: &Node{
			key: "",
			value: "",
			forward: make([]*Node, maxLevel),
		},
		level: 0,
		rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}


func (s *SkipList) randomLevel() int {
	level := 0
	for s.rnd.Float32() < probability && level < maxLevel-1 {
		level++
	}
	return level
}

func (s *SkipList) Search(key string) (string, bool){
	currentNode := s.head
	// Start from the highest level and work down
	for i := s.level; i>=0; i--{
		for currentNode.forward[i] != nil && currentNode.forward[i].key < key {
			currentNode = currentNode.forward[i]
		}
	}
	currentNode = currentNode.forward[0]
	if currentNode != nil && currentNode.key == key {
		return currentNode.value, true
	}
	return "", false
}

func (s *SkipList) Insert(key string, value string){
	update := make([]*Node, maxLevel)
	currentNode := s.head
	
	// Traverse down from the current highest level
	for i:=s.level; i>=0; i--{
		for currentNode.forward[i] != nil && currentNode.forward[i].key < key {
			currentNode = currentNode.forward[i]
		}
		update[i] = currentNode
	}
	
	// Check if we're at level 0 (bottom)
	currentNode = currentNode.forward[0]
	if currentNode != nil && currentNode.key == key {
		currentNode.value = value
		return
	}
	
	// Generate random level for the new node
	level := s.randomLevel()
	
	// If new level is higher than current max level, update pointers
	if level > s.level {
		for i:=s.level+1; i<=level; i++ {
			update[i] = s.head
		}
		s.level=level
	}
	
	// Create new node
	newNode := &Node{
		key: key,
		value: value,
		forward: make([]*Node, level+1),
	}
	
	// Insert the new node by updating pointers
	for i:=0; i<=level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
}

func (s *SkipList) Delete(key string) error{
	update := make([]*Node, maxLevel)
	currentNode := s.head
	for i:=s.level;i>=0;i--{
		for currentNode.forward[i] != nil && currentNode.forward[i].key < key {
			currentNode = currentNode.forward[i]
		}
		update[i] = currentNode
	}
	currentNode = currentNode.forward[0]
	if currentNode != nil && currentNode.key == key {
		for i:=0; i<=s.level; i++ {
			if update[i].forward[i] != currentNode {
				break
			}
			update[i].forward[i] = currentNode.forward[i]
		}
		// Reduce level if top levels are now empty
		for s.level>0 && s.head.forward[s.level] == nil {
			s.level--
		}
		return nil
	}
	return fmt.Errorf("key not found")
}


func (s *SkipList) Iterator() []*Node {
    var nodes []*Node
    current := s.head.forward[0]
    for current != nil {
        nodes = append(nodes, current)
        current = current.forward[0]
    }
    return nodes
}

func printSkipList(s *SkipList) {
	fmt.Println("\n---------SkipList--------")
	for i := s.level; i >= 0; i-- {
		fmt.Printf("Level %d: ", i)
		iter := s.head  // Reset for each level
		for iter.forward[i] != nil {
			// Changed to %s for key, %d for offset
			fmt.Printf("%s:%d -> ", iter.forward[i].key, iter.forward[i].value)
			iter = iter.forward[i]
		}
		fmt.Println("nil")
	}
	fmt.Println("-------------------------")
}
func serializeRecord(key string, value string, op byte)[]byte{
	keyBytes := []byte(key)
	valueBytes := []byte(value)

	// timestamp := uint64(time.Now().UnixNano())

	keySize := uint32(len(keyBytes))
	valueSize := uint32(len(valueBytes))
	// timestampSize := uint64(timestamp)

	totalSize := 1+4 + 4 + len(keyBytes) + len(valueBytes)

	record := make([]byte, totalSize)
	record[0] = op
	binary.BigEndian.PutUint32(record[1:5], keySize)
	binary.BigEndian.PutUint32(record[5:9], valueSize)
	copy(record[9:], keyBytes)
	copy(record[9+len(keyBytes):], valueBytes)

	return record
}

func deserializeSSTableRecord(record []byte)(string, string, error){
	keySize := binary.BigEndian.Uint32(record[0:4])
	valueSize := binary.BigEndian.Uint32(record[4:8])
	key := string(record[8:8+keySize])
	value := string(record[8+keySize:8+keySize+valueSize])
	return key, value, nil
}

func deserializeRecord(record []byte)(string, string){
	// op := record[0]
	keySize := binary.BigEndian.Uint32(record[1:5])
	valueSize := binary.BigEndian.Uint32(record[5:9])
	key := string(record[5:5+keySize])
	value := string(record[5+keySize:5+keySize+valueSize])
	return key, value
}



func main(){
	filename := "wal.log"
	memTable := newMemTable()
	// Example 1: Add some test data
	err:=memTable.ReplayWAL(filename)
	if err != nil {
		log.Fatal(err)
		return
	}
	memTable.Set("key1", "value1")
	memTable.Set("key2", "value2")
	memTable.Set("key3", "value3")
	value, ok := memTable.Get("key1")
	if ok {
		fmt.Println("Value:", value)
	} else {
		fmt.Println("Key not found")
	}
	memTable.Delete("key1")
	value, ok = memTable.Get("key1")
	if ok {
		fmt.Println("Value:", value)
	} else {
		fmt.Println("Key not found")
	}

}