package testutil

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestRenameHook_PreRenameHookRenameTest(t *testing.T) {
	//init action
	original := filepath.Join(string(filepath.Separator), "tmp", fmt.Sprintf("dev-%d", time.Now().Unix()))
	mountpoint := filepath.Join(string(filepath.Separator), "tmp", fmt.Sprintf("mountpoint-%d", time.Now().Unix()))
	server := newFuseServer(t, original, mountpoint)
	//remember to call unmount after you do not use it
	defer cleanUp(server)

	//normal logic
	log.Print(filepath.Join(mountpoint, "tsdb.txt"))
	_, err := os.Create(filepath.Join(mountpoint, "tsdb.txt"))

	if err != nil {
		log.Printf("%v",err)
	}

	//rename should be failed
	err = os.Rename(filepath.Join(mountpoint, "tsdb.txt"), filepath.Join(mountpoint, "tsdbNew.txt"))
	NotOk(t, err)
	fmt.Println(err)
}

func newFuseServer(t *testing.T, original,mountpoint string)(*fuse.Server){
	createDirIfAbsent(original)
	createDirIfAbsent(mountpoint)
	fs, err :=  NewHookFs(original, mountpoint, &TestRenameHook{})
	Ok(t, err)
	server, err := fs.NewServe()
	if err != nil {
		log.Fatalf("start server failed, %v", err)
	}
	Ok(t, err)
	go func(){
		fs.Start(server)
	}()

	return server
}

func cleanUp(server *fuse.Server) {
	err := server.Unmount()
	if err != nil {
		log.Fatal("umount failed, please umount the mountpoint by the command `fusermount -u $unmountpoint`", err)
	}
}

func createDirIfAbsent(name string) {
	 _, err := os.Stat(name)
	 if err != nil {
	 	os.Mkdir(name, os.ModePerm)
	 }
}

type TestRenameHook struct {}

func (h *TestRenameHook) PreRename(oldPatgh string, newPath string) (hooked bool, err error) {
	log.Printf("renamed file from %s to %s", oldPatgh, newPath)
	return true, syscall.EIO
}
func (h *TestRenameHook) PostRename(oldPatgh string, newPath string) (hooked bool, err error) {
	return false, nil
}

