package tools

import (
	"encoding/hex"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/streamingfast/dstore"
	badgerKVDB "github.com/streamingfast/kvdb/store/badger"
	"github.com/streamingfast/substreams/manifest"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"github.com/streamingfast/substreams/storage/store"
)

var badgerLoadCmd = &cobra.Command{
	Use:   "load <manifest_path> <module_name> <store_url> <badger_url>",
	Short: "loads a substreams store into a badger store",
	Args:  cobra.ExactArgs(2),
	RunE:  BadgerLoadE,
}

func BadgerLoadE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	manifestPath := args[0]
	moduleName := args[1]
	storePath := args[2]

	badgerPath := args[3]

	baseDStore, err := dstore.NewStore(storePath, "", "", false)
	if err != nil {
		return fmt.Errorf("creating base store: %w", err)
	}

	manifestReader := manifest.NewReader(manifestPath)
	pkg, err := manifestReader.Read()
	if err != nil {
		return fmt.Errorf("read manifest %q: %w", manifestPath, err)
	}

	graph, err := manifest.NewModuleGraph(pkg.Modules.Modules)
	if err != nil {
		return fmt.Errorf("creating module graph: %w", err)
	}

	var module *pbsubstreams.Module

	hashes := manifest.NewModuleHashes()
	for _, mod := range pkg.Modules.Modules {
		if mod.Name != moduleName {
			continue
		}

		module = mod
	}

	if module == nil {
		return fmt.Errorf("module %q not found", moduleName)
	}

	hash := hex.EncodeToString(hashes.HashModule(pkg.Modules, module, graph))

	conf, err := store.NewConfig(
		module.Name,
		module.InitialBlock,
		hash,
		module.GetKind().(*pbsubstreams.Module_KindStore_).KindStore.UpdatePolicy,
		module.GetKind().(*pbsubstreams.Module_KindStore_).KindStore.ValueType,
		baseDStore,
	)
	if err != nil {
		return fmt.Errorf("creating store config: %w", err)
	}

	badger, err := badgerKVDB.NewStore(badgerPath)
	if err != nil {
		return fmt.Errorf("creating badger store: %w", err)
	}

	kvStore := conf.NewFullKV(zlog)
	kvStore.Iter(func(key string, value []byte) error {
		err := badger.Put(ctx, []byte(key), value)
		if err != nil {
			return fmt.Errorf("putting key %q: %w", key, err)
		}
		return nil
	})

	err = badger.FlushPuts(ctx)
	if err != nil {
		return fmt.Errorf("flushing puts: %w", err)
	}

	return nil
}
