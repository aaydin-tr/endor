package fslock

import (
	"errors"
	"os"
	"sync"

	"golang.org/x/sys/windows"
)

type FSLock struct {
	file    os.File
	mu      sync.RWMutex
	handler windows.Handle
}

const (
	reserved = 0
	allBytes = uint32(windows.INFINITE)
)

var (
	EOF             = errors.New("EOF")
	defaultFileMode = windows.O_APPEND | windows.O_RDWR
)

func NewFSLock(fileName string, mode int) (*FSLock, error) {
	if mode == 0 {
		mode = defaultFileMode
	}

	f, err := os.OpenFile(fileName, mode, 0666)
	if err != nil {
		return nil, err
	}
	fs := &FSLock{file: *f, mu: sync.RWMutex{}, handler: windows.Handle(f.Fd())}

	ol, err := newOverlapped()
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(ol.HEvent)
	err = windows.LockFileEx(fs.handler, windows.LOCKFILE_EXCLUSIVE_LOCK, reserved, allBytes, allBytes, ol)
	if err != nil && err != windows.ERROR_IO_PENDING {
		return nil, err
	}

	s, err := windows.WaitForSingleObject(ol.HEvent, uint32(windows.INFINITE))
	switch s {
	case windows.WAIT_OBJECT_0:
		return fs, nil
	default:
		return nil, err
	}
}

func (f *FSLock) Unlock() error {
	return windows.CloseHandle(f.handler)
}

func (f *FSLock) Write(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	done := uint32(0)
	return windows.WriteFile(f.handler, data, &done, nil)
}

func (f *FSLock) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return windows.FlushFileBuffers(f.handler)
}

func (f *FSLock) Close() error {
	return windows.CloseHandle(f.handler)
}

func (f *FSLock) Read() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	fileInfo := windows.ByHandleFileInformation{}
	err := windows.GetFileInformationByHandle(f.handler, &fileInfo)
	if err != nil {
		return nil, err
	}

	data := make([]byte, fileInfo.FileSizeLow+1)
	var n uint32
	ov, err := newOverlapped()
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(ov.HEvent)

	err = windows.ReadFile(f.handler, data, &n, ov)
	if err != nil && err == windows.ERROR_IO_PENDING {
		if err = windows.GetOverlappedResult(f.handler, ov, &n, true); err != nil {
			return nil, err
		}
	}

	return data[:n], nil
}

func (f *FSLock) ReadAtToEndOfLine(offset int64, length int) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	data := make([]byte, length)
	var n uint32
	ov, err := newOverlappedWithOffset(uint32(offset))
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(ov.HEvent)

	err = windows.ReadFile(f.handler, data, &n, ov)
	if err != nil && err == windows.ERROR_IO_PENDING {
		if err = windows.GetOverlappedResult(f.handler, ov, &n, true); err != nil {
			return nil, err
		}
	}

	if n == 0 {
		return nil, EOF
	}

	// TODO last char is \n
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			return data[:i], nil
		}
	}

	return f.ReadAtToEndOfLine(offset, length*2)
}

func newOverlapped() (*windows.Overlapped, error) {
	manualReset := uint32(1)
	initialState := uint32(0)
	event, err := windows.CreateEvent(nil, manualReset, initialState, nil)
	if err != nil {
		return nil, err
	}
	return &windows.Overlapped{HEvent: event}, nil
}

func newOverlappedWithOffset(offest uint32) (*windows.Overlapped, error) {
	manualReset := uint32(1)
	initialState := uint32(0)
	event, err := windows.CreateEvent(nil, manualReset, initialState, nil)
	if err != nil {
		return nil, err
	}
	return &windows.Overlapped{
		HEvent: event,
		Offset: offest,
	}, nil
}
