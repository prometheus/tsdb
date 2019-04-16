package testutil

type Hook interface{}

type HookContext interface{}

type HookOnRename interface {
	PreRename(oldPatgh string, newPath string) (hooked bool, err error)
	PostRename(oldPatgh string, newPath string) (hooked bool, err error)
}
