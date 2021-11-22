package keeper

import (
	"bytes"
	"encoding/binary"

	"github.com/CosmWasm/wasmd/x/wasm/types"

	"github.com/cosmos/cosmos-sdk/store/prefix"
	storetypes "github.com/cosmos/cosmos-sdk/store/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
)

type WasmSnapshot struct {
	keeper *Keeper
	multi  storetypes.CommitMultiStore
}

func NewWasmSnapshot(multi storetypes.CommitMultiStore, keeper *Keeper) *WasmSnapshot {
	return &WasmSnapshot{
		keeper: keeper,
		multi:  multi,
	}
}

func (ws WasmSnapshot) GetItems() ([]storetypes.SnapshotCustomItem, error) {
	items := []storetypes.SnapshotCustomItem{}
	store := ws.multi.GetKVStore(ws.keeper.storeKey)
	prefixStore := prefix.NewStore(store, types.CodeKeyPrefix)
	iter := prefixStore.Iterator(nil, nil)
	for ; iter.Valid(); iter.Next() {
		var c types.CodeInfo
		ws.keeper.cdc.MustUnmarshal(iter.Value(), &c)
		codeID := binary.BigEndian.Uint64(iter.Key())
		// cb returns true to stop early
		bytecode, err := ws.GetByteCode(store, codeID)
		if err != nil {
			return nil, err
		}

		items = append(items, storetypes.SnapshotCustomItem{
			Key:   string(c.CodeHash),
			Value: bytecode,
		})
	}

	return items, nil
}

func (ws WasmSnapshot) RestoreItem(item *storetypes.SnapshotCustomItem) error {
	newCodeHash, err := ws.keeper.wasmVM.Create(item.Value)
	if err != nil {
		return sdkerrors.Wrap(types.ErrCreateFailed, err.Error())
	}
	if !bytes.Equal([]byte(item.Key), newCodeHash) {
		return sdkerrors.Wrap(types.ErrInvalid, "code hashes not same")
	}

	return nil
}

func (ws WasmSnapshot) GetByteCode(store sdk.KVStore, codeID uint64) ([]byte, error) {
	var codeInfo types.CodeInfo
	codeInfoBz := store.Get(types.GetCodeKey(codeID))
	if codeInfoBz == nil {
		return nil, nil
	}
	ws.keeper.cdc.MustUnmarshal(codeInfoBz, &codeInfo)
	return ws.keeper.wasmVM.GetCode(codeInfo.CodeHash)
}
