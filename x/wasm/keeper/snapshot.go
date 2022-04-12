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

	store := cacheMS.GetKVStore(ws.storeKey)
	prefixStore := prefix.NewStore(store, types.CodeKeyPrefix)
	iter := prefixStore.Iterator(nil, nil)
	defer iter.Close()

	hashes := make(map[string]bool)
	var items []types.SnapshotWasmItem
	for ; iter.Valid(); iter.Next() {
		var c types.CodeInfo
		ws.keeper.cdc.MustUnmarshal(iter.Value(), &c)
		codeID := binary.BigEndian.Uint64(iter.Key())

		if _, ok := hashes[string(c.CodeHash)]; ok {
			continue
		}
		hashes[string(c.CodeHash)] = true

		bytecode, err := ws.keeper.wasmVM.GetCode(c.CodeHash)
		if err != nil {
			return err
		}

		item := types.SnapshotWasmItem{
			CodeID:       codeID,
			WASMByteCode: bytecode,
		}
		items = append(items, item)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].CodeID < items[j].CodeID
	})

	for _, item := range items {
		data, err := ws.keeper.cdc.Marshal(&item)
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

		// Check pin code
		if !store.Has(types.GetPinnedCodeIndexPrefix(wasmItem.CodeID)) {
			continue
		}

		if err := ws.keeper.wasmVM.Pin(codeHash); err != nil {
			return snapshottypes.SnapshotItem{}, sdkerrors.Wrap(types.ErrPinContractFailed, err.Error())
		}
	}

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
