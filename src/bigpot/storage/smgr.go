package storage

import (
	"fmt"
	"os"

	"bigpot/system"
)

type Smgr interface {
	GetRelation(reln system.RelFileNode) SmgrRelation
}

type SmgrRelation interface {
	NBlocks() (system.BlockNumber, error)
	Read(blockNum system.BlockNumber, data *Block) error
	Write(blockNum system.BlockNumber, data *Block) error
	Extend(blockNum system.BlockNumber, data *Block) error
	Close()
}

// Implements Smgr
type mdSmgr struct {
	lookup map[system.RelFileNode]*mdRelation
}

// Implements SmgrRelation
type mdRelation struct {
	node system.RelFileNode
	file *os.File
}

func NewMdSmgr() Smgr {
	mgr := &mdSmgr{
		lookup: map[system.RelFileNode]*mdRelation{},
	}
	return mgr
}

func (mgr *mdSmgr) GetRelation(reln system.RelFileNode) SmgrRelation {
	if rel, found := mgr.lookup[reln]; found {
		return SmgrRelation(rel)
	}

	rel := &mdRelation{
		node: reln,
	}
	mgr.lookup[reln] = rel

	return SmgrRelation(rel)
}

func (md *mdRelation) NBlocks() (system.BlockNumber, error) {
	relpath := system.RelPath(md.node)

	fi, err := os.Stat(relpath)
	if err != nil {
		return 0, err
	}
	return system.BlockNumber(fi.Size() / int64(system.BlockSize)), nil
}

func (md *mdRelation) openFile() error {
	// TODO: do we care concurrency?
	if md.file == nil {
		relpath := system.RelPath(md.node)
		file, err := os.OpenFile(relpath, os.O_RDWR, 0600)
		if err != nil {
			md.file = nil
			return err
		}
		md.file = file
	}

	return nil
}

func (md *mdRelation) Read(blockNum system.BlockNumber, data *Block) error {
	if err := md.openFile(); err != nil {
		return err
	}
	defer md.Close()

	pos := int64(blockNum * system.BlockSize)
	if _, err := md.file.Seek(pos, os.SEEK_SET); err != nil {
		return err
	}

	// TODO: ReadFull
	if _, err := md.file.Read(data[:]); err != nil {
		return err
	}
	return nil
}

func (md *mdRelation) Write(blockNum system.BlockNumber, data *Block) error {
	if err := md.openFile(); err != nil {
		return err
	}
	// TODO:
	defer md.Close()

	pos := int64(blockNum * system.BlockSize)
	if _, err := md.file.Seek(pos, os.SEEK_SET); err != nil {
		return err
	}

	if _, err := md.file.Write(data[:]); err != nil {
		return err
	}

	return nil
}

func (md *mdRelation) Extend(blockNum system.BlockNumber, data *Block) error {
	if err := md.openFile(); err != nil {
		return err
	}
	defer md.Close()

	pos := int64(blockNum * system.BlockSize)
	if posres, err := md.file.Seek(0, os.SEEK_END); err != nil {
		return err
	} else if posres != pos {
		return fmt.Errorf("could not seek to block %d", blockNum)
	}

	if _, err := md.file.Write(data[:]); err != nil {
		return err
	}

	return nil
}

func (md *mdRelation) Close() {
	if md.file != nil {
		md.file.Close()
		md.file = nil
	}
}
