package keeper

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"

	"github.com/CosmWasm/wasmd/x/wasm/types"

	snapshottypes "github.com/cosmos/cosmos-sdk/snapshots/types"
	"github.com/cosmos/cosmos-sdk/store/prefix"
	storetypes "github.com/cosmos/cosmos-sdk/store/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	protoio "github.com/gogo/protobuf/io"
)

var _ snapshottypes.ExtensionSnapshotter = (*Snapshotter)(nil)

type Snapshotter struct {
	cms    storetypes.MultiStore
	keeper *Keeper
}

func NewSnapshotter(cms storetypes.MultiStore, keeper *Keeper) *Snapshotter {
	return &Snapshotter{
		cms:    cms,
		keeper: keeper,
	}
}

// SnapshotName implements types.ExtensionSnapshotter
func (*Snapshotter) SnapshotName() string {
	return types.ModuleName
}

// SnapshotFormat implements types.ExtensionSnapshotter
func (*Snapshotter) SnapshotFormat() uint32 {
	return types.SnapshotFormat
}

// SupportedFormats implements types.ExtensionSnapshotter
func (*Snapshotter) SupportedFormats() []uint32 {
	return []uint32{types.SnapshotFormat}
}

// Snapshot implements types.Snapshotter
func (ws *Snapshotter) Snapshot(height uint64, protoWriter protoio.Writer) error {
	cacheMS, err := ws.cms.CacheMultiStoreWithVersion(int64(height))
	if err != nil {
		return err
	}

	prefixStore := prefix.NewStore(cacheMS.GetKVStore(ws.keeper.storeKey), types.CodeKeyPrefix)
	iter := prefixStore.Iterator(nil, nil)
	defer iter.Close()

	type codeItem struct {
		CodeID   uint64
		CodeHash []byte
	}
	uniqueHashes := make(map[string]bool)
	codeItems := []codeItem{}

	for ; iter.Valid(); iter.Next() {
		var c types.CodeInfo
		ws.keeper.cdc.MustUnmarshal(iter.Value(), &c)
		codeID := binary.BigEndian.Uint64(iter.Key())

		if _, ok := uniqueHashes[string(c.CodeHash)]; ok {
			continue
		}
		uniqueHashes[string(c.CodeHash)] = true

		codeItems = append(codeItems, codeItem{
			CodeID:   codeID,
			CodeHash: c.CodeHash,
		})
	}

	sort.SliceStable(codeItems, func(i, j int) bool {
		return codeItems[i].CodeID < codeItems[j].CodeID
	})

	for _, item := range codeItems {
		bytecode, err := ws.keeper.wasmVM.GetCode(item.CodeHash)
		if err != nil {
			return err
		}

		wasmItem := types.SnapshotWasmItem{
			CodeHash:     item.CodeHash,
			WASMByteCode: bytecode,
		}

		data, err := ws.keeper.cdc.Marshal(&wasmItem)
		if err != nil {
			return sdkerrors.Wrap(err, "cannot encode protobuf wasm message")
		}

		snapshottypes.WriteExtensionItem(protoWriter, data)
	}

	return nil
}

// Restore implements types.Snapshotter
func (ws Snapshotter) Restore(height uint64, format uint32, protoReader protoio.Reader) (snapshottypes.SnapshotItem, error) {
	if format != types.SnapshotFormat {
		return snapshottypes.SnapshotItem{}, snapshottypes.ErrUnknownFormat
	}

	var snapshotItem snapshottypes.SnapshotItem
	for {
		snapshotItem = snapshottypes.SnapshotItem{}
		err := protoReader.ReadMsg(&snapshotItem)
		if err == io.EOF {
			break
		} else if err != nil {
			return snapshottypes.SnapshotItem{}, sdkerrors.Wrap(err, "invalid protobuf message")
		}

		extension := snapshotItem.GetExtensionPayload()
		// break and return item to process other modules.
		if extension == nil {
			break
		}

		var wasmItem types.SnapshotWasmItem
		err = ws.keeper.cdc.Unmarshal(extension.Payload, &wasmItem)
		if err != nil {
			return snapshottypes.SnapshotItem{}, sdkerrors.Wrap(err, "invalid protobuf wasm message")
		}

		codeHash, err := ws.keeper.wasmVM.Create(wasmItem.WASMByteCode)
		if err != nil {
			return snapshottypes.SnapshotItem{}, sdkerrors.Wrap(types.ErrCreateFailed, err.Error())
		}

		if !bytes.Equal(wasmItem.CodeHash, codeHash) {
			return snapshottypes.SnapshotItem{}, sdkerrors.Wrap(types.ErrInvalid, "code hashes not same")
		}
	}

	return snapshotItem, nil
}

func (ws Snapshotter) getCodeInfo(store storetypes.KVStore, codeID uint64) *types.CodeInfo {
	var codeInfo types.CodeInfo
	codeInfoBz := store.Get(types.GetCodeKey(codeID))
	if codeInfoBz == nil {
		return nil
	}
	ws.keeper.cdc.MustUnmarshal(codeInfoBz, &codeInfo)
	return &codeInfo
}
