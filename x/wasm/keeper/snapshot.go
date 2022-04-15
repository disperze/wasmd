package keeper

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/CosmWasm/wasmd/x/wasm/types"

	snapshottypes "github.com/cosmos/cosmos-sdk/snapshots/types"
	"github.com/cosmos/cosmos-sdk/store/prefix"
	storetypes "github.com/cosmos/cosmos-sdk/store/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	protoio "github.com/gogo/protobuf/io"
)

var _ snapshottypes.ExtensionSnapshotter = (*WasmSnapshotter)(nil)

type WasmSnapshotter struct {
	cms      storetypes.MultiStore
	storeKey storetypes.StoreKey
	keeper   *Keeper
}

func NewWasmSnapshotter(cms storetypes.MultiStore, storeKey storetypes.StoreKey, keeper *Keeper) *WasmSnapshotter {
	return &WasmSnapshotter{
		cms:      cms,
		storeKey: storeKey,
		keeper:   keeper,
	}
}

// SnapshotName implements types.ExtensionSnapshotter
func (*WasmSnapshotter) SnapshotName() string {
	return types.ModuleName
}

// SnapshotFormat implements types.ExtensionSnapshotter
func (*WasmSnapshotter) SnapshotFormat() uint32 {
	return types.SnapshotFormat
}

// SupportedFormats implements types.ExtensionSnapshotter
func (*WasmSnapshotter) SupportedFormats() []uint32 {
	return []uint32{types.SnapshotFormat}
}

// Snapshot implements types.Snapshotter
func (ws *WasmSnapshotter) Snapshot(height uint64, protoWriter protoio.Writer) error {
	cacheMS, err := ws.cms.CacheMultiStoreWithVersion(int64(height))
	if err != nil {
		return err
	}

	prefixStore := prefix.NewStore(cacheMS.GetKVStore(ws.storeKey), types.CodeKeyPrefix)
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

	sort.Slice(codeItems, func(i, j int) bool {
		return codeItems[i].CodeID < codeItems[j].CodeID
	})

	for _, item := range codeItems {
		bytecode, err := ws.keeper.wasmVM.GetCode(item.CodeHash)
		if err != nil {
			return err
		}

		wasmItem := types.SnapshotWasmItem{
			CodeID:       item.CodeID,
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
func (ws WasmSnapshotter) Restore(height uint64, format uint32, protoReader protoio.Reader) (snapshottypes.SnapshotItem, error) {
	var snapshotItem snapshottypes.SnapshotItem
	cacheMS, err := ws.cms.CacheMultiStoreWithVersion(int64(height))
	if err != nil {
		return snapshotItem, err
	}

	store := cacheMS.GetKVStore(ws.storeKey)
	for {
		snapshotItem = snapshottypes.SnapshotItem{}
		err := protoReader.ReadMsg(&snapshotItem)
		if err != nil {
			return snapshottypes.SnapshotItem{}, sdkerrors.Wrap(err, "invalid protobuf message")
		}

		extension := snapshotItem.GetExtensionPayload()
		if extension == nil {
			break
		}

		var wasmItem types.SnapshotWasmItem
		err = ws.keeper.cdc.Unmarshal(extension.Payload, &wasmItem)
		if err != nil {
			return snapshottypes.SnapshotItem{}, sdkerrors.Wrap(err, "invalid protobuf wasm message")
		}

		var codeInfo = ws.getCodeInfo(store, wasmItem.CodeID)
		if codeInfo == nil {
			return snapshottypes.SnapshotItem{}, sdkerrors.Wrap(types.ErrInvalid, "code info not found")
		}

		codeHash, err := ws.keeper.wasmVM.Create(wasmItem.WASMByteCode)
		if err != nil {
			return snapshottypes.SnapshotItem{}, sdkerrors.Wrap(types.ErrCreateFailed, err.Error())
		}

		if !bytes.Equal(codeInfo.CodeHash, codeHash) {
			return snapshottypes.SnapshotItem{}, sdkerrors.Wrap(types.ErrInvalid, "code hashes not same")
		}
	}
	// TODO: Initialize pin codes

	return snapshotItem, nil
}

func (ws WasmSnapshotter) getCodeInfo(store storetypes.KVStore, codeID uint64) *types.CodeInfo {
	var codeInfo types.CodeInfo
	codeInfoBz := store.Get(types.GetCodeKey(codeID))
	if codeInfoBz == nil {
		return nil
	}
	ws.keeper.cdc.MustUnmarshal(codeInfoBz, &codeInfo)
	return &codeInfo
}
