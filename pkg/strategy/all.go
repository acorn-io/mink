package strategy

type CompleteStrategy interface {
	Creater
	Updater
	StatusUpdater
	Getter
	Lister
	Deleter
	Watcher

	Destroy()
}

type AugmentComplete func(s CompleteStrategy) (CompleteStrategy, error)
